"""
Tablet Report Service for YugabyteDB Log Analyzer

This service parses tablet report files and inserts parsed data into PostgreSQL tables.
"""

import os
from pathlib import Path
from typing import List, Dict, Any
import logging
import uuid
import json
from utils.exceptions import AnalysisError, DatabaseError
from config.settings import settings
from services.database_service import DatabaseService

logger = logging.getLogger(__name__)

class TabletReportService:
    """
    Service for parsing and storing tablet report data.
    """
    def __init__(self):
        self.db = DatabaseService()

    def parse(self, bundle_dir: Path) -> Dict[str, Any]:
        """
        Parse tablet report files from the support bundle directory.
        Returns a dict with parsed data for each table.
        """
        try:
            universe_file = next(bundle_dir.rglob('*universe-details.json'))
            entity_file = next(bundle_dir.rglob('*dump-entities.json'))
            tablet_report_files = list(bundle_dir.rglob('*tablet_report.json'))
        except StopIteration:
            raise AnalysisError("Missing required universe-details.json or dump-entities.json in bundle.")

        # Parse universe details
        with open(universe_file, 'r', encoding='utf-8') as f:
            universe_data = json.load(f)
        nodes = {}
        for node_detail in universe_data.get('nodeDetailsSet', []):
            private_ip = node_detail.get('cloudInfo', {}).get('private_ip')
            if not private_ip:
                continue
            nodes[private_ip] = {
                'nodeName': node_detail.get('nodeName'),
                'nodeUuid': node_detail.get('nodeUuid', '').replace('-', ''),
                'private_ip': private_ip,
                'az': node_detail.get('cloudInfo', {}).get('az'),
                'region': node_detail.get('cloudInfo', {}).get('region'),
                'tserverRpcPort': node_detail.get('tserverRpcPort'),
                'tserver_uuid': node_detail.get('nodeUuid', '').replace('-', '')
            }

        # Parse dump entities to correct tserver UUIDs
        with open(entity_file, 'r', encoding='utf-8') as f:
            entity_data = json.load(f)
        for t in entity_data.get('tablets', []):
            for r in t.get('replicas', []):
                ip, port = r.get('addr', ':').split(':')
                if ip in nodes:
                    nodes[ip]['tserver_uuid'] = r.get('server_uuid')

        # Prepare parsed data containers
        cluster_rows = []
        for node in nodes.values():
            cluster_rows.append({
                'type': 'TSERVER',
                'uuid': node['tserver_uuid'],
                'ip': node['private_ip'],
                'port': node['tserverRpcPort'],
                'region': node['region'],
                'zone': node['az'],
                'role': None,
                'uptime': None  # Use None for SQL NULL (interval)
            })

        tablets_rows = []
        name_to_uuid = {n['nodeName']: n['tserver_uuid'] for n in nodes.values() if n.get('nodeName')}
        for file_path in tablet_report_files:
            node_uuid = None
            for node_name, tserver_uuid in name_to_uuid.items():
                if file_path.name.startswith(node_name):
                    node_uuid = tserver_uuid
                    break
            if not node_uuid:
                logger.warning(f"Could not determine node UUID for report '{file_path.name}'. Skipping.")
                continue
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                decoder = json.JSONDecoder()
                pos = 0
                while pos < len(content):
                    try:
                        obj, size = decoder.raw_decode(content, pos)
                        pos += size
                        while pos < len(content) and content[pos].isspace():
                            pos += 1
                        for t_data in obj.get('content', []):
                            status = t_data.get('tablet', {}).get('tablet_status', {})
                            cstate = t_data.get('consensus_state', {}).get('cstate', {})
                            tablets_rows.append({
                                'node_uuid': node_uuid,
                                'tablet_uuid': status.get('tabletId'),
                                'table_name': status.get('tableName'),
                                'table_uuid': status.get('tableId'),
                                'namespace': status.get('namespaceName'),
                                'state': status.get('state'),
                                'status': status.get('tabletDataState'),
                                'start_key': status.get('partition', {}).get('partitionKeyStart'),
                                'end_key': status.get('partition', {}).get('partitionKeyEnd'),
                                'sst_size': status.get('sstFilesDiskSize'),
                                'wal_size': status.get('walFilesDiskSize'),
                                'cterm': cstate.get('currentTerm'),
                                'cidx': cstate.get('config', {}).get('opidIndex'),
                                'leader': cstate.get('leaderUuid'),
                                'lease_status': t_data.get('consensus_state', {}).get('leaderLeaseStatus')
                            })
                    except json.JSONDecodeError:
                        break
        # Aggregate tableinfo
        table_stats = {}
        for row in tablets_rows:
            key = (row['namespace'], row['table_name'])
            if key not in table_stats:
                table_stats[key] = {
                    "tablet_uuids": set(), "node_counts": {},
                    "SST_TOT_BYTES": 0, "WAL_TOT_BYTES": 0, "LEADER_TABLETS": 0,
                    "TOT_TABLET_COUNT": 0, "TABLE_UUID": None
                }
            stats = table_stats[key]
            stats["tablet_uuids"].add(row['tablet_uuid'])
            stats["node_counts"].setdefault(row['node_uuid'], 0)
            stats["node_counts"][row['node_uuid']] += 1
            try:
                sst_val = int(row['sst_size']) if row['sst_size'] is not None else 0
            except (ValueError, TypeError):
                sst_val = 0
            try:
                wal_val = int(row['wal_size']) if row['wal_size'] is not None else 0
            except (ValueError, TypeError):
                wal_val = 0
            stats["SST_TOT_BYTES"] += sst_val
            stats["WAL_TOT_BYTES"] += wal_val
            stats["TOT_TABLET_COUNT"] += 1
            if row['table_uuid']:
                stats["TABLE_UUID"] = row['table_uuid']
            if row['lease_status'] == 'HAS_LEASE':
                stats["LEADER_TABLETS"] += 1

        def format_bytes(size_bytes):
            if not size_bytes:
                return "0B"
            size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
            import math
            i = int(math.floor(math.log(size_bytes, 1024))) if size_bytes > 0 else 0
            p = math.pow(1024, i)
            s = round(size_bytes / p, 2)
            return f"{s}{size_name[i]}"

        tableinfo_rows = []
        for (ns, name), stats in table_stats.items():
            uniq_count = len(stats["tablet_uuids"])
            total_count = stats["TOT_TABLET_COUNT"]
            node_counts = list(stats["node_counts"].values())
            sst_rf1_bytes = (stats["SST_TOT_BYTES"] * uniq_count / total_count) if total_count else 0
            tableinfo_rows.append({
                'namespace': ns,
                'tablename': name,
                'table_uuid': stats.get("TABLE_UUID"),
                'tot_tablet_count': total_count,
                'uniq_tablet_count': uniq_count,
                'uniq_tablets_estimate': 0,
                'leader_tablets': stats["LEADER_TABLETS"],
                'node_tablet_min': min(node_counts) if node_counts else 0,
                'node_tablet_max': max(node_counts) if node_counts else 0,
                'keys_per_tablet': 0,
                'key_range_overlap': 0,
                'unmatched_key_size': 0,
                'comment': "",
                'sst_tot_bytes': stats["SST_TOT_BYTES"],
                'wal_tot_bytes': stats["WAL_TOT_BYTES"],
                'sst_tot_human': format_bytes(stats["SST_TOT_BYTES"]),
                'wal_tot_human': format_bytes(stats["WAL_TOT_BYTES"]),
                'sst_rf1_human': format_bytes(sst_rf1_bytes),
                'tot_human': format_bytes(stats["SST_TOT_BYTES"] + stats["WAL_TOT_BYTES"])
            })

        # Aggregate region_zone_tablets
        tserver_map = {row['uuid']: {'region': row['region'], 'zone': row['zone']} for row in cluster_rows if row['type'] == 'TSERVER'}
        valid_tablets = [row for row in tablets_rows if row['status'] != 'TABLET_DATA_TOMBSTONED']
        all_unique_tablets = {row['tablet_uuid'] for row in valid_tablets}
        from collections import defaultdict
        zone_stats = defaultdict(lambda: {
            'tservers': set(),
            'tablet_replicas': defaultdict(int)
        })
        for row in valid_tablets:
            node_uuid = row['node_uuid']
            tablet_uuid = row['tablet_uuid']
            if node_uuid in tserver_map:
                info = tserver_map[node_uuid]
                key = (info['region'], info['zone'])
                zone_stats[key]['tservers'].add(node_uuid)
                zone_stats[key]['tablet_replicas'][tablet_uuid] += 1
        max_replicas = max((max(v['tablet_replicas'].values()) for v in zone_stats.values() if v['tablet_replicas']), default=1)
        region_zone_rows = []
        for (region, zone), stats in zone_stats.items():
            replicas_in_zone = stats['tablet_replicas']
            replica_counts_by_number = defaultdict(int)
            for count in replicas_in_zone.values():
                replica_counts_by_number[count] += 1
            missing_count = len(all_unique_tablets - set(replicas_in_zone.keys()))
            row = {
                'region': region,
                'zone': zone,
                'tservers': len(stats['tservers']),
                'missing_replicas': str(missing_count),
            }
            for i in range(1, max_replicas + 1):
                row[f'{i}_replicas'] = str(replica_counts_by_number[i])
            row['balanced'] = "YES" if len(set(replicas_in_zone.values())) <= 1 else "NO"
            region_zone_rows.append(row)

        return {
            'cluster': cluster_rows,
            'tablets': tablets_rows,
            'tableinfo': tableinfo_rows,
            'region_zone_tablets': region_zone_rows
        }

    def insert_to_db(self, report_id: str, parsed_data: Dict[str, Any]) -> None:
        """
        Insert parsed tablet report data into the database tables.
        """
        from psycopg2.extras import execute_values
        with self.db.get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    # Bulk insert cluster rows
                    cluster_values = [
                        (report_id, row['type'], row['uuid'], row['ip'], row['port'], row['region'], row['zone'], row['role'], row['uptime'])
                        for row in parsed_data['cluster']
                    ]
                    if cluster_values:
                        execute_values(
                            cur,
                            """
                            INSERT INTO tablet_report_cluster (report_id, type, uuid, ip, port, region, zone, role, uptime)
                            VALUES %s
                            """,
                            cluster_values
                        )
                    # Bulk insert tablet rows
                    tablet_values = [
                        (report_id, row['node_uuid'], row['tablet_uuid'], row['table_name'], row['table_uuid'], row['namespace'], row['state'], row['status'], row['start_key'], row['end_key'], row['sst_size'], row['wal_size'], row['cterm'], row['cidx'], row['leader'], row['lease_status'])
                        for row in parsed_data['tablets']
                    ]
                    if tablet_values:
                        execute_values(
                            cur,
                            """
                            INSERT INTO tablet_report_tablets (report_id, node_uuid, tablet_uuid, table_name, table_uuid, namespace, state, status, start_key, end_key, sst_size, wal_size, cterm, cidx, leader, lease_status)
                            VALUES %s
                            """,
                            tablet_values
                        )
                    # Bulk insert tableinfo rows
                    tableinfo_values = [
                        (
                            report_id, row['namespace'], row['tablename'], row['table_uuid'], row['tot_tablet_count'],
                            row['uniq_tablet_count'], row['uniq_tablets_estimate'], row['leader_tablets'], row['node_tablet_min'],
                            row['node_tablet_max'], row['keys_per_tablet'], row['key_range_overlap'], row['unmatched_key_size'],
                            row['comment'], row['sst_tot_bytes'], row['wal_tot_bytes'], row['sst_tot_human'], row['wal_tot_human'],
                            row['sst_rf1_human'], row['tot_human']
                        )
                        for row in parsed_data.get('tableinfo', [])
                    ]
                    if tableinfo_values:
                        execute_values(
                            cur,
                            """
                            INSERT INTO tablet_report_tableinfo (
                                report_id, namespace, tablename, table_uuid, tot_tablet_count, uniq_tablet_count,
                                uniq_tablets_estimate, leader_tablets, node_tablet_min, node_tablet_max, keys_per_tablet,
                                key_range_overlap, unmatched_key_size, comment, sst_tot_bytes, wal_tot_bytes,
                                sst_tot_human, wal_tot_human, sst_rf1_human, tot_human
                            ) VALUES %s
                            """,
                            tableinfo_values
                        )
                    # Bulk insert region_zone_tablets rows
                    region_zone_values = [
                        (
                            report_id, row['region'], row['zone'], row['tservers'], row['missing_replicas'],
                            row.get('1_replicas', '0'), row['balanced']
                        )
                        for row in parsed_data.get('region_zone_tablets', [])
                    ]
                    if region_zone_values:
                        execute_values(
                            cur,
                            """
                            INSERT INTO tablet_report_region_zone_tablets (
                                report_id, region, zone, tservers, missing_replicas, "1_replicas", balanced
                            ) VALUES %s
                            """,
                            region_zone_values
                        )
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to insert tablet report data: {e}")
                raise DatabaseError(f"Failed to insert tablet report data: {e}")
