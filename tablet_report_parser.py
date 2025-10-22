#!/usr/bin/env python3
##########################################################################
## Python Tablet Report Parser (Simplified)
##
##   Parses JSON tablet reports from a YugabyteDB support bundle and
##   creates an SQLite database with a focused set of tables.
##
##   Version: 2.2
##   Original Perl Script Author: Unnamed
##   Python Conversion: Gemini
##########################################################################

import argparse
import sqlite3
import json
import base64
import sys
from pathlib import Path
import time
import gzip
from collections import defaultdict
import math

__version__ = "2.2"

def get_simplified_schema(max_replicas_per_zone=1):
    """
    Returns the SQL schema string with dynamic columns for region_zone_tablets.
    """
    # Dynamically create columns like "[1_replicas]", "[2_replicas]", etc.
    # SQLite identifiers containing special characters should be quoted with brackets.
    replica_columns = ',\n'.join(
        f"        [{i}_replicas] TEXT" for i in range(1, max_replicas_per_zone + 1)
    )

    return f"""
-- Core data tables
CREATE TABLE cluster(type, uuid TEXT PRIMARY KEY, ip, port, region, zone, role, uptime);

CREATE TABLE tablet (
    node_uuid TEXT, tablet_uuid TEXT, table_name TEXT, table_uuid TEXT, namespace TEXT,
    state TEXT, status TEXT, start_key TEXT, end_key TEXT, sst_size INTEGER,
    wal_size INTEGER, cterm INTEGER, cidx INTEGER, leader TEXT, lease_status TEXT
);
CREATE INDEX tablet_uuid_idx ON tablet (tablet_uuid);
CREATE INDEX tablet_table_idx ON tablet (namespace, table_name);

-- Tables required for processing but not for final output
CREATE TABLE ENT_KEYSPACE (id TEXT PRIMARY KEY,name,type);
CREATE TABLE ENT_TABLE (id TEXT PRIMARY KEY, keyspace_id,state, table_name);
CREATE TABLE ENT_TABLET (id TEXT ,table_id,state,is_leader,server_uuid,server_addr,type);


-- Analytical tables populated by the script
CREATE TABLE tableinfo(
    NAMESPACE TEXT, TABLENAME TEXT, TABLE_UUID TEXT, TOT_TABLET_COUNT INTEGER,
    UNIQ_TABLET_COUNT INTEGER, UNIQ_TABLETS_ESTIMATE INTEGER, LEADER_TABLETS INTEGER,
    NODE_TABLET_MIN INTEGER, NODE_TABLET_MAX INTEGER, KEYS_PER_TABLET INTEGER,
    KEY_RANGE_OVERLAP INTEGER, UNMATCHED_KEY_SIZE INTEGER, COMMENT TEXT,
    SST_TOT_BYTES INTEGER, WAL_TOT_BYTES INTEGER, SST_TOT_HUMAN TEXT,
    WAL_TOT_HUMAN TEXT, SST_RF1_HUMAN TEXT, TOT_HUMAN TEXT
);

CREATE TABLE region_zone_tablets(
        region TEXT,
        zone TEXT,
        tservers INTEGER,
        missing_replicas TEXT,
{replica_columns},
        balanced TEXT
);
"""

def format_bytes(size_bytes):
    """Converts a size in bytes to a human-readable string."""
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1024))) if size_bytes > 0 else 0
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s}{size_name[i]}"

def decode_uuid(b64_string):
    """Decodes a base64 encoded UUID string and returns its hex representation."""
    if not b64_string:
        return None
    try:
        return base64.b64decode(b64_string).hex()
    except (TypeError, base64.binascii.Error):
        return b64_string

def decode_partition_key(b64_string, default_hex):
    """Decodes a base64 partition key, returning a '0x...' hex string."""
    if not b64_string:
        return f"0x{default_hex}"
    try:
        return f"0x{base64.b64decode(b64_string).hex()}"
    except (TypeError, base64.binascii.Error):
        return f"0x{default_hex}"

def open_file(file_path):
    """Opens a file, transparently handling .gz compression."""
    return gzip.open(file_path, 'rt', encoding='utf-8') if file_path.suffix == '.gz' else open(file_path, 'r', encoding='utf-8')

def parse_universe_details(file_path):
    """Parses universe-details.json to get node information."""
    print(f"   -> Processing universe details from {file_path.name}...")
    nodes = {}
    with open_file(file_path) as f:
        data = json.load(f)

    for node_detail in data.get('nodeDetailsSet', []):
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
            'tserver_uuid': node_detail.get('nodeUuid', '').replace('-', '') # Will be corrected
        }
    print(f"      ...found {len(nodes)} nodes.")
    return nodes

def parse_dump_entities(cursor, file_path, universe_nodes):
    """Parses dump-entities.json to correct tserver UUIDs and populate cluster table."""
    print(f"   -> Processing entities from {file_path.name}...")
    with open_file(file_path) as f:
        data = json.load(f)

    for t in data.get('tablets', []):
        for r in t.get('replicas', []):
            ip, port = r.get('addr', ':').split(':')
            if ip in universe_nodes:
                universe_nodes[ip]['tserver_uuid'] = r.get('server_uuid')

    cluster_tservers = [
        ('TSERVER', node['tserver_uuid'], node['private_ip'], node['tserverRpcPort'],
         node['region'], node['az'], None, 0)
        for node in universe_nodes.values()
    ]
    cursor.executemany("INSERT INTO cluster(type, uuid, ip, port, region, zone, role, uptime) VALUES(?,?,?,?,?,?,?,?)", cluster_tservers)
    print(f"      ...populated 'cluster' table with {len(cluster_tservers)} T-Servers.")

def parse_tablet_reports(cursor, file_paths, universe_nodes):
    """Parses one or more tablet_report.json files."""
    print(f"   -> Processing {len(file_paths)} tablet report file(s)...")
    
    # Create a lookup from the node name to its corrected tserver_uuid
    name_to_uuid_lookup = {
        node_data['nodeName']: node_data['tserver_uuid']
        for node_data in universe_nodes.values() if 'nodeName' in node_data and node_data.get('nodeName')
    }
    
    total_tablets_processed = 0

    for file_path in file_paths:
        file_tablets = 0
        node_uuid = None
        
        # Find the corresponding node_uuid by matching the filename with the node name.
        # The tablet report filename usually starts with the node name.
        for node_name, tserver_uuid in name_to_uuid_lookup.items():
            if file_path.name.startswith(node_name):
                node_uuid = tserver_uuid
                break

        if not node_uuid:
            print(f"      - WARNING: Could not determine node UUID for report '{file_path.name}'. Skipping.")
            continue

        print(f"      - Parsing {file_path.name} for node {node_uuid}")
        with open_file(file_path) as f:
            content = f.read()
            decoder = json.JSONDecoder()
            pos = 0
            while pos < len(content):
                try:
                    obj, size = decoder.raw_decode(content, pos)
                    pos += size
                    while pos < len(content) and content[pos].isspace():
                        pos += 1

                    tablets_to_insert = []
                    for t_data in obj.get('content', []):
                        status = t_data.get('tablet', {}).get('tablet_status', {})
                        cstate = t_data.get('consensus_state', {}).get('cstate', {})
                        tablets_to_insert.append((
                            node_uuid, status.get('tabletId'), status.get('tableName'),
                            status.get('tableId'), status.get('namespaceName'), status.get('state'),
                            status.get('tabletDataState'),
                            decode_partition_key(status.get('partition', {}).get('partitionKeyStart'), '0000'),
                            decode_partition_key(status.get('partition', {}).get('partitionKeyEnd'), 'ffff'),
                            status.get('sstFilesDiskSize'), status.get('walFilesDiskSize'),
                            cstate.get('currentTerm'), cstate.get('config', {}).get('opidIndex'),
                            decode_uuid(cstate.get('leaderUuid')),
                            t_data.get('consensus_state', {}).get('leaderLeaseStatus')
                        ))

                    if tablets_to_insert:
                        cursor.executemany("INSERT INTO tablet VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", tablets_to_insert)
                        file_tablets += len(tablets_to_insert)
                except json.JSONDecodeError:
                    break
        total_tablets_processed += file_tablets
        print(f"        ...processed {file_tablets} tablet replicas.")
    print(f"      ...total of {total_tablets_processed} tablet replicas processed from all reports.")
    return total_tablets_processed > 0

def calculate_and_populate_tableinfo(cursor):
    """Reads the raw tablet data and populates the aggregated tableinfo table."""
    print("   -> Calculating table-level statistics for 'tableinfo'...")
    
    table_stats = {}
    cursor.execute("SELECT namespace, table_name, table_uuid, tablet_uuid, node_uuid, sst_size, wal_size, lease_status FROM tablet")
    rows = cursor.fetchall()
    
    for ns, name, table_id, t_uuid, n_uuid, sst, wal, lease in rows:
        key = (ns, name)
        if key not in table_stats:
            table_stats[key] = {
                "tablet_uuids": set(), "node_counts": defaultdict(int),
                "SST_TOT_BYTES": 0, "WAL_TOT_BYTES": 0, "LEADER_TABLETS": 0,
                "TOT_TABLET_COUNT": 0, "TABLE_UUID": None
            }
        
        stats = table_stats[key]
        stats["tablet_uuids"].add(t_uuid)
        stats["node_counts"][n_uuid] += 1
        stats["SST_TOT_BYTES"] += sst or 0
        stats["WAL_TOT_BYTES"] += wal or 0
        stats["TOT_TABLET_COUNT"] += 1
        if table_id:
            stats["TABLE_UUID"] = table_id
        if lease == 'HAS_LEASE':
            stats["LEADER_TABLETS"] += 1
    
    tableinfo_rows = []
    for (ns, name), stats in table_stats.items():
        uniq_count = len(stats["tablet_uuids"])
        total_count = stats["TOT_TABLET_COUNT"]
        node_counts = stats["node_counts"].values()
        
        sst_rf1_bytes = (stats["SST_TOT_BYTES"] * uniq_count / total_count) if total_count else 0

        tableinfo_rows.append((
            ns, name, stats.get("TABLE_UUID"), total_count, uniq_count,
            0, # UNIQ_TABLETS_ESTIMATE
            stats["LEADER_TABLETS"],
            min(node_counts) if node_counts else 0,
            max(node_counts) if node_counts else 0,
            0, 0, 0, "", # KEY_*, COMMENT
            stats["SST_TOT_BYTES"], stats["WAL_TOT_BYTES"],
            format_bytes(stats["SST_TOT_BYTES"]), format_bytes(stats["WAL_TOT_BYTES"]),
            format_bytes(sst_rf1_bytes), format_bytes(stats["SST_TOT_BYTES"] + stats["WAL_TOT_BYTES"])
        ))
        
    cursor.executemany("INSERT INTO tableinfo VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", tableinfo_rows)
    print(f"      ...populated 'tableinfo' with {len(tableinfo_rows)} records.")

def get_region_zone_create_sql(max_replicas):
    """Returns the full CREATE TABLE statement for the region_zone_tablets table."""
    full_schema = get_simplified_schema(max_replicas)
    start_index = full_schema.find("CREATE TABLE region_zone_tablets")
    end_index = full_schema.find(";", start_index)
    return full_schema[start_index : end_index + 1]

def calculate_and_populate_region_zone(cursor):
    """Calculates and populates the region_zone_tablets table."""
    print("   -> Calculating region/zone distribution...")
    
    tserver_map = {
        row[0]: {'region': row[1], 'zone': row[2]}
        for row in cursor.execute("SELECT uuid, region, zone FROM cluster WHERE type='TSERVER'").fetchall()
    }
    
    rows = cursor.execute("SELECT node_uuid, tablet_uuid FROM tablet WHERE status != 'TABLET_DATA_TOMBSTONED'").fetchall()
    
    all_unique_tablets = {row[1] for row in rows}
    
    zone_stats = defaultdict(lambda: {
        'tservers': set(),
        'tablet_replicas': defaultdict(int) # {tablet_uuid: count}
    })
    
    for node_uuid, tablet_uuid in rows:
        if node_uuid in tserver_map:
            info = tserver_map[node_uuid]
            key = (info['region'], info['zone'])
            zone_stats[key]['tservers'].add(node_uuid)
            zone_stats[key]['tablet_replicas'][tablet_uuid] += 1
    
    max_replicas = max((max(v['tablet_replicas'].values()) for v in zone_stats.values() if v['tablet_replicas']), default=1)

    # Re-create schema with correct number of columns before inserting
    cursor.execute("DROP TABLE region_zone_tablets")
    cursor.execute(get_region_zone_create_sql(max_replicas))

    rz_rows = []
    for (region, zone), stats in zone_stats.items():
        replicas_in_zone = stats['tablet_replicas']
        replica_counts_by_number = defaultdict(int)
        for count in replicas_in_zone.values():
            replica_counts_by_number[count] += 1

        missing_count = len(all_unique_tablets - set(replicas_in_zone.keys()))
        
        row = [region, zone, len(stats['tservers']), str(missing_count)]
        row.extend(str(replica_counts_by_number[i]) for i in range(1, max_replicas + 1))
        
        # Balance check: balanced if all tablets in the zone have the same replica count
        is_balanced = "YES" if len(set(replicas_in_zone.values())) <= 1 else "NO"
        row.append(is_balanced)
        rz_rows.append(tuple(row))

    cols = "region, zone, tservers, missing_replicas, " + \
           ', '.join(f"[{i}_replicas]" for i in range(1, max_replicas + 1)) + \
           ", balanced"
    placeholders = ','.join(['?'] * len(cols.split(',')))
    
    cursor.executemany(f"INSERT INTO region_zone_tablets ({cols}) VALUES ({placeholders})", rz_rows)
    print(f"      ...populated 'region_zone_tablets' with {len(rz_rows)} records.")
    return True

def main():
    parser = argparse.ArgumentParser(description=f"YugabyteDB Support Bundle Tablet Report Parser (Python v{__version__})")
    parser.add_argument("bundle_dir", help="Path to the extracted support bundle directory.")
    parser.add_argument("-o", "--output", help="Name of the output SQLite database file. Defaults to '[bundle_dir_name].sqlite'.")
    args = parser.parse_args()

    bundle_path = Path(args.bundle_dir)
    if not bundle_path.is_dir():
        sys.exit(f"Error: Path '{args.bundle_dir}' is not a valid directory.")

    output_file = Path(args.output) if args.output else Path(f"{bundle_path.name}.sqlite")

    if output_file.exists():
        mtime = output_file.stat().st_mtime
        backup_name = output_file.with_suffix(f".{time.strftime('%Y-%m-%d', time.localtime(mtime))}.sqlite")
        print(f"WARNING: Output file '{output_file}' already exists.")
        try:
            output_file.rename(backup_name)
            print(f"         Renamed existing file to '{backup_name}'.")
        except OSError as e:
            sys.exit(f"Error: Could not rename existing file: {e}")

    print(f"Starting parser. Output will be saved to '{output_file}'")

    universe_files = list(bundle_path.rglob('*universe-details.json*'))
    entity_files = list(bundle_path.rglob('*dump-entities.json*'))
    tablet_report_files = list(bundle_path.rglob('*tablet_report.json*'))

    if not universe_files or not entity_files:
        sys.exit("Error: Could not find 'universe-details.json' or 'dump-entities.json'.")

    try:
        with sqlite3.connect(output_file) as conn:
            cursor = conn.cursor()
            conn.row_factory = sqlite3.Row # Makes rows dict-like

            print("\n[1/3] Creating database schema...")
            # Create a dummy version of region_zone_tablets first. It will be recreated later.
            cursor.executescript(get_simplified_schema(1))

            print("\n[2/3] Parsing raw data...")
            universe_nodes = parse_universe_details(universe_files[0])
            parse_dump_entities(cursor, entity_files[0], universe_nodes)
            
            tablets_found = False
            if tablet_report_files:
                tablets_found = parse_tablet_reports(cursor, tablet_report_files, universe_nodes)
            else:
                print("   -> No tablet report files (*tablet_report.json) found to process.")
            
            conn.commit()

            print("\n[3/3] Populating analytical tables...")
            if tablets_found:
                calculate_and_populate_tableinfo(cursor)
                calculate_and_populate_region_zone(cursor)
                conn.commit()
            else:
                 print("   -> Skipping analytical tables as no tablet data was loaded.")


    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        if output_file.exists():
            output_file.unlink()
        sys.exit(1)

    print("\n--- Processing Complete! ---")
    print(f"Database saved to: {output_file}")

if __name__ == "__main__":
    main()