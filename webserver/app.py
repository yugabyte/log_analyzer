"""
Refactored Flask web server for Log Analyzer.

This module provides a clean, maintainable web interface for viewing
log analysis reports with proper error handling and type hints.
"""

import sys
import os
import sqlite3
import math
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import logging

from flask import Flask, render_template, request, redirect, url_for, jsonify, send_from_directory
from werkzeug.exceptions import NotFound, BadRequest

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import settings
from services.database_service import DatabaseService
from utils.logging_config import setup_logging, get_logger
from utils.exceptions import DatabaseError


class LogAnalyzerWebApp:
    """Main web application class."""
    
    def __init__(self):
        self.app = Flask(__name__)
        self.db_service = DatabaseService()
        self.logger = get_logger("web_app")
        
        # Configure Flask
        self.app.config['SECRET_KEY'] = 'your-secret-key-here'
        self.app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size
        
        # Register routes
        self._register_routes()
        
        # Set up error handlers
        self._setup_error_handlers()
    
    def _register_routes(self) -> None:
        """Register all application routes."""
        
        @self.app.route('/')
        def index():
            """Main page showing list of reports."""
            try:
                page = int(request.args.get('page', 1))
                per_page = 10
                
                reports_data = self.db_service.get_reports_list(
                    page=page,
                    per_page=per_page
                )
                
                return render_template(
                    'index.html',
                    reports=reports_data['reports'],
                    page=reports_data['page'],
                    total_pages=reports_data['total_pages']
                )
                
            except DatabaseError as e:
                self.logger.error(f"Database error in index: {e}")
                return render_template('index.html', reports=[], page=1, total_pages=1)
            except Exception as e:
                self.logger.error(f"Unexpected error in index: {e}")
                return render_template('index.html', reports=[], page=1, total_pages=1)
        
        @self.app.route('/img/<path:filename>')
        def serve_img(filename):
            """Serve static images."""
            return send_from_directory(
                Path(self.app.root_path) / 'img', 
                filename
            )
        
        @self.app.route('/reports/<uuid>')
        def report_page(uuid):
            """Report viewing page."""
            try:
                # Load log patterns and solutions
                from lib.patterns_lib import universe_regex_patterns, pg_regex_patterns, solutions
                # Build a mapping: pattern -> name for all log messages
                pattern_to_name = {}
                for name, pattern in universe_regex_patterns.items():
                    pattern_to_name[pattern] = name
                for name, pattern in pg_regex_patterns.items():
                    pattern_to_name[pattern] = name

                # Pass both solutions and pattern_to_name to frontend
                return render_template(
                    'reports.html',
                    report_uuid=uuid,
                    log_solutions_map=solutions,
                    pattern_to_name_map=pattern_to_name
                )
            except Exception as e:
                self.logger.error(f"Error loading report page: {e}")
                raise NotFound("Report page not found")
        
        @self.app.route('/api/reports/<uuid>')
        def get_report_api(uuid):
            """API endpoint to get report data."""
            try:
                report_data = self.db_service.get_report(uuid)
                if report_data:
                    return jsonify(report_data)
                else:
                    return jsonify({'error': 'Report not found'}), 404
                    
            except DatabaseError as e:
                self.logger.error(f"Database error getting report: {e}")
                return jsonify({'error': 'Database error'}), 500
            except Exception as e:
                self.logger.error(f"Unexpected error getting report: {e}")
                return jsonify({'error': 'Internal server error'}), 500
        
        @self.app.route('/api/histogram/<report_id>')
        def histogram_api(report_id):
            """API endpoint for histogram data with filtering."""
            try:
                # Get query parameters
                interval = int(request.args.get('interval', 1))
                start = request.args.get('start')
                end = request.args.get('end')
                
                # Validate interval
                if interval not in [1, 5, 15, 30, 60]:
                    raise BadRequest("Invalid interval value")
                
                # Get report data
                report_data = self.db_service.get_report(report_id)
                if not report_data:
                    return jsonify({'error': 'Report not found'}), 404
                
                # Filter and aggregate histogram data
                filtered_data = self._filter_histogram_data(
                    report_data, start, end, interval
                )
                
                return jsonify(filtered_data)
                
            except BadRequest as e:
                return jsonify({'error': str(e)}), 400
            except DatabaseError as e:
                self.logger.error(f"Database error in histogram API: {e}")
                return jsonify({'error': 'Database error'}), 500
            except Exception as e:
                self.logger.error(f"Unexpected error in histogram API: {e}")
                return jsonify({'error': 'Internal server error'}), 500
        
        @self.app.route('/api/gflags/<uuid>')
        def gflags_api(uuid):
            """API endpoint for GFlags data."""
            try:
                gflags_data = self.db_service.get_gflags(uuid)
                return jsonify(gflags_data)
                
            except DatabaseError as e:
                self.logger.error(f"Database error getting GFlags: {e}")
                return jsonify({'error': 'Database error'}), 500
            except Exception as e:
                self.logger.error(f"Unexpected error getting GFlags: {e}")
                return jsonify({'error': 'Internal server error'}), 500
        
        @self.app.route('/api/gflags_diff/<cluster_name>/<organization>')
        def gflags_diff_api(cluster_name, organization):
            """API endpoint for GFlags diff from support bundles of a universe."""
            try:
                # Get 'days' from query param, default 90
                days = request.args.get('days', default=90, type=int)
                bundle_name = request.args.get('bundle')
                with self.db_service.get_connection() as conn:
                    with conn.cursor() as cur:
                        # If bundle_name is provided, get its timestamp, else use latest
                        if bundle_name:
                            cur.execute(
                                """
                                SELECT "timestamp" FROM public.support_bundle_header
                                WHERE support_bundle = %s AND cluster_name = %s AND organization = %s
                                """,
                                (bundle_name, cluster_name, organization)
                            )
                            ts_row = cur.fetchone()
                            if not ts_row or not ts_row[0]:
                                return jsonify({'error': 'Support bundle not found'}), 404
                            ref_ts = ts_row[0]
                        else:
                            cur.execute(
                                """
                                SELECT MAX("timestamp") FROM public.support_bundle_header
                                WHERE cluster_name = %s AND organization = %s
                                """,
                                (cluster_name, organization)
                            )
                            max_ts_row = cur.fetchone()
                            if not max_ts_row or not max_ts_row[0]:
                                return jsonify({'error': 'No support bundles found'}), 404
                            ref_ts = max_ts_row[0]

                        # Get all bundles in the last N days from the reference bundle
                        cur.execute(
                            """
                            SELECT support_bundle, "timestamp", cluster_uuid
                            FROM public.support_bundle_header
                            WHERE cluster_name = %s AND organization = %s
                              AND "timestamp" >= %s::timestamp - INTERVAL '%s days'
                              AND "timestamp" <= %s::timestamp
                            ORDER BY "timestamp" DESC
                            """,
                            (cluster_name, organization, ref_ts, days, ref_ts)
                        )
                        bundles = cur.fetchall()
                        if not bundles:
                            return jsonify({'error': 'No support bundles found'}), 404

                        bundle_names = [b[0] for b in bundles]  # newest to oldest
                        bundle_timestamps = [b[1].isoformat() for b in bundles]
                        cluster_uuid = bundles[0][2]  # Get cluster_uuid from first result

                        # Fetch all GFlags for these bundles
                        cur.execute(
                            """
                            SELECT support_bundle, node_name, server_type, gflag, value
                            FROM public.support_bundle_gflags
                            WHERE support_bundle = ANY(%s)
                            ORDER BY support_bundle, node_name, server_type, gflag
                            """,
                            (bundle_names,)
                        )
                        rows = cur.fetchall()

                # Organize: {bundle: {node: {role: {flag: value}}}}
                bundle_gflags = {}
                for bundle, node, role, flag, value in rows:
                    bundle_gflags.setdefault(bundle, {}).setdefault(node, {}).setdefault(role, {})[flag] = value

                # For each node/role, build a list of gflags per bundle
                node_role_keys = set()
                for bundle in bundle_names:
                    for node in bundle_gflags.get(bundle, {}):
                        for role in bundle_gflags[bundle][node]:
                            node_role_keys.add((node, role))

                # For each node/role, for each bundle, get gflags dict
                diffs = {}
                for node, role in sorted(node_role_keys):
                    diffs.setdefault(node, {})[role] = []
                    prev = None
                    for i, bundle in enumerate(bundle_names):
                        gflags = bundle_gflags.get(bundle, {}).get(node, {}).get(role, {})
                        # Compare to previous
                        if prev is None:
                            change = { 'type': 'initial', 'gflags': gflags }
                        else:
                            change = self._compare_gflags(prev, gflags)
                        diffs[node][role].append({
                            'bundle': bundle,
                            'timestamp': bundle_timestamps[i],
                            'change': change,
                            'gflags': gflags
                        })
                        prev = gflags

                return jsonify({
                    'universe': cluster_uuid,
                    'organization': organization,
                    'bundles': [
                        {'name': b, 'timestamp': t} for b, t in zip(bundle_names, bundle_timestamps)
                    ],
                    'diffs': diffs
                })
            except Exception as e:
                self.logger.error(f"Error in gflags_diff_api: {e}")
                return jsonify({'error': 'Internal server error'}), 500
        
        @self.app.route('/api/related_reports/<uuid>')
        def related_reports_api(uuid):
            """API endpoint for related reports."""
            try:
                related_data = self.db_service.get_related_reports(uuid)
                return jsonify(related_data)
                
            except DatabaseError as e:
                self.logger.error(f"Database error getting related reports: {e}")
                return jsonify({'error': 'Database error'}), 500
            except Exception as e:
                self.logger.error(f"Unexpected error getting related reports: {e}")
                return jsonify({'error': 'Internal server error'}), 500
        
        @self.app.route('/api/search_reports')
        def search_reports():
            """API endpoint for searching reports."""
            try:
                query = request.args.get('q', '').strip()
                page = int(request.args.get('page', 1))
                per_page = int(request.args.get('per_page', 10))
                
                if not query:
                    return jsonify({
                        "reports": [], 
                        "page": 1, 
                        "total_pages": 1
                    })
                
                search_results = self.db_service.get_reports_list(
                    page=page,
                    per_page=per_page,
                    search_query=query
                )
                
                return jsonify(search_results)
                
            except DatabaseError as e:
                self.logger.error(f"Database error in search: {e}")
                return jsonify({'error': 'Database error'}), 500
            except Exception as e:
                self.logger.error(f"Unexpected error in search: {e}")
                return jsonify({'error': 'Internal server error'}), 500
        
        @self.app.route('/api/node_info/<uuid>')
        def node_info_api(uuid):
            """API endpoint for node information."""
            try:
                node_data = self.db_service.get_node_info(uuid)
                return jsonify(node_data)
                
            except DatabaseError as e:
                self.logger.error(f"Database error getting node info: {e}")
                return jsonify({'error': 'Database error'}), 500
            except Exception as e:
                self.logger.error(f"Unexpected error getting node info: {e}")
                return jsonify({'error': 'Internal server error'}), 500
        
        @self.app.route('/api/long_operations/<uuid>')
        def long_operations_api(uuid):
            """API endpoint for long operations data."""
            try:
                report_data = self.db_service.get_report(uuid)
                if not report_data:
                    return jsonify({'error': 'Report not found'}), 404
                
                long_operations = report_data.get('long_operations', {})
                return jsonify({'long_operations': long_operations})
                
            except DatabaseError as e:
                self.logger.error(f"Database error getting long operations: {e}")
                return jsonify({'error': 'Database error'}), 500
            except Exception as e:
                self.logger.error(f"Unexpected error getting long operations: {e}")
                return jsonify({'error': 'Internal server error'}), 500
        
        @self.app.route('/api/histogram_latest_datetime/<report_id>')
        def histogram_latest_datetime_api(report_id):
            """API endpoint for latest datetime in histogram."""
            try:
                report_data = self.db_service.get_report(report_id)
                if not report_data:
                    return jsonify({'error': 'Report not found'}), 404
                
                latest_datetime = self._get_latest_histogram_datetime(report_data)
                
                return jsonify({'latest_datetime': latest_datetime})
                
            except DatabaseError as e:
                self.logger.error(f"Database error getting latest datetime: {e}")
                return jsonify({'error': 'Database error'}), 500
            except Exception as e:
                self.logger.error(f"Unexpected error getting latest datetime: {e}")
                return jsonify({'error': 'Internal server error'}), 500
        
        @self.app.route('/api/reports/<uuid>', methods=['DELETE'])
        def delete_report_api(uuid):
            """API endpoint to delete a report by UUID."""
            try:
                deleted = self.db_service.delete_report(uuid)
                if deleted:
                    return jsonify({'success': True}), 200
                else:
                    return jsonify({'error': 'Report not found'}), 404
            except DatabaseError as e:
                self.logger.error(f"Database error deleting report: {e}")
                return jsonify({'error': 'Database error'}), 500
            except Exception as e:
                self.logger.error(f"Unexpected error deleting report: {e}")
                return jsonify({'error': 'Internal server error'}), 500

        @self.app.route('/tablet-report')
        def tablet_report_page():
            """Tablet report page that visualizes a SQLite tablet report database."""
            db_path = request.args.get('db', '').strip()
            if not db_path:
                return render_template('tablet_report.html', error='No database path provided. Use ?db=/path/to/file.sqlite')
            db_file = Path(db_path)
            if not db_file.exists():
                return render_template('tablet_report.html', error=f'Database file not found: {db_path}')
            if not db_file.suffix == '.sqlite':
                return render_template('tablet_report.html', error='File must be a .sqlite database')
            return render_template('tablet_report.html', db_path=db_path, db_name=db_file.stem)

        @self.app.route('/api/tablet-report/data')
        def tablet_report_data_api():
            """API endpoint returning all tablet report data from a SQLite DB."""
            db_path = request.args.get('db', '').strip()
            if not db_path:
                return jsonify({'error': 'No database path provided'}), 400
            db_file = Path(db_path)
            if not db_file.exists():
                return jsonify({'error': f'Database file not found: {db_path}'}), 404
            try:
                data = self._query_tablet_report_db(db_path)
                return jsonify(data)
            except Exception as e:
                self.logger.error(f"Error querying tablet report DB: {e}")
                return jsonify({'error': str(e)}), 500

        @self.app.route('/api/tablet-report/table-tablet-sizes')
        def tablet_report_table_sizes_api():
            """Return individual tablet SST sizes for a specific table."""
            db_path = request.args.get('db', '').strip()
            namespace = request.args.get('namespace', '').strip()
            table_name = request.args.get('table', '').strip()
            if not db_path or not namespace or not table_name:
                return jsonify({'error': 'db, namespace, and table are required'}), 400
            if not Path(db_path).exists():
                return jsonify({'error': 'Database file not found'}), 404
            try:
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                SST = "CASE WHEN typeof(sst_size)='integer' THEN sst_size ELSE 0 END"
                cur.execute(f"""
                    SELECT t.tablet_uuid, t.node_uuid, {SST} as sst_val,
                           CASE WHEN t.node_uuid = t.leader AND t.lease_status='HAS_LEASE'
                                THEN 1 ELSE 0 END as is_leader,
                           c.ip as node_ip, c.zone as node_zone
                    FROM tablet t
                    LEFT JOIN cluster c ON c.uuid = t.node_uuid AND c.type='TSERVER'
                    WHERE t.namespace = ? AND t.table_name = ?
                      AND typeof(t.sst_size)='integer' AND t.sst_size > 0
                    ORDER BY sst_val DESC
                """, (namespace, table_name))
                rows = [dict(r) for r in cur.fetchall()]
                conn.close()
                return jsonify({'tablets': rows, 'namespace': namespace, 'table': table_name})
            except Exception as e:
                self.logger.error(f"Error querying table tablet sizes: {e}")
                return jsonify({'error': str(e)}), 500
    
    def _setup_error_handlers(self) -> None:
        """Set up error handlers for the application."""
        
        @self.app.errorhandler(404)
        def not_found(error):
            return render_template('404.html'), 404
        
        @self.app.errorhandler(500)
        def internal_error(error):
            self.logger.error(f"Internal server error: {error}")
            return render_template('500.html'), 500
        
        @self.app.errorhandler(BadRequest)
        def bad_request(error):
            return jsonify({'error': str(error)}), 400
    
    def _filter_histogram_data(
        self,
        data: Dict[str, Any],
        start: Optional[str],
        end: Optional[str],
        interval: int
    ) -> Dict[str, Any]:
        """Filter and aggregate histogram data, robust to null/invalid keys."""
        def parse_time(s: str) -> Optional[datetime]:
            if not s or s == 'null':
                return None
            try:
                return datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ')
            except Exception:
                return None
        def format_time(dt: datetime) -> str:
            return dt.strftime('%Y-%m-%dT%H:%M:00Z')
        # If no start/end provided, compute last 7 days from latest bucket
        if not start or not end:
            all_bucket_times = []
            for node, node_data in data.get('nodes', {}).items():
                for proc, proc_data in node_data.items():
                    for msg, msg_stats in proc_data.get('logMessages', {}).items():
                        hist = msg_stats.get('histogram', {})
                        all_bucket_times.extend([k for k in hist.keys() if k and k != 'null'])
            valid_dates = [parse_time(b) for b in all_bucket_times]
            valid_dates = [d for d in valid_dates if d]
            if valid_dates:
                max_date = max(valid_dates)
                min_date = max_date - timedelta(days=7)
                if not start:
                    start_dt = min_date
                else:
                    start_dt = parse_time(start)
                if not end:
                    end_dt = max_date
                else:
                    end_dt = parse_time(end)
            else:
                start_dt = None
                end_dt = None
        else:
            start_dt = parse_time(start)
            end_dt = parse_time(end)
        # Filter and aggregate data
        for node, node_data in data.get('nodes', {}).items():
            for proc, proc_data in node_data.items():
                for msg, msg_stats in proc_data.get('logMessages', {}).items():
                    hist = msg_stats.get('histogram', {})
                    # Filter by time range
                    filtered = {}
                    for k, v in hist.items():
                        t = parse_time(k)
                        if t and (not start_dt or t >= start_dt) and (not end_dt or t <= end_dt):
                            filtered[k] = v
                    # Aggregate by interval
                    if interval > 1:
                        agg = {}
                        for k, v in filtered.items():
                            t = parse_time(k)
                            if not t:
                                continue
                            bucket_minute = (t.minute // interval) * interval
                            bucket = t.replace(minute=bucket_minute, second=0, microsecond=0)
                            bucket_key = format_time(bucket)
                            agg[bucket_key] = agg.get(bucket_key, 0) + v
                        msg_stats['histogram'] = agg
                    else:
                        msg_stats['histogram'] = filtered
        return data
    
    def _get_latest_histogram_datetime(self, data: Dict[str, Any]) -> Optional[str]:
        """Get the latest datetime from histogram data, robust to null/invalid keys."""
        all_bucket_times = []
        for node, node_data in data.get('nodes', {}).items():
            for proc, proc_data in node_data.items():
                for msg, msg_stats in proc_data.get('logMessages', {}).items():
                    hist = msg_stats.get('histogram', {})
                    all_bucket_times.extend([k for k in hist.keys() if k and k != 'null'])
        valid_dates = []
        for b in all_bucket_times:
            try:
                valid_dates.append(datetime.strptime(b, '%Y-%m-%dT%H:%M:%SZ'))
            except Exception:
                continue
        if not valid_dates:
            return None
        max_date = max(valid_dates)
        return max_date.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    def _compare_gflags(self, prev: Dict[str, Any], curr: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compare two gflags dicts. Return dict with added, removed, modified.
        Handles all value types as string for comparison.
        """
        added = {}
        removed = {}
        modified = {}
        prev_keys = set(prev.keys())
        curr_keys = set(curr.keys())
        
        # Added flags
        for k in curr_keys - prev_keys:
            added[k] = curr[k]
        
        # Removed flags
        for k in prev_keys - curr_keys:
            removed[k] = prev[k]
        
        # Modified flags
        for k in prev_keys & curr_keys:
            if str(prev[k]) != str(curr[k]):
                modified[k] = {'old': prev[k], 'new': curr[k]}
        
        return {
            'type': 'diff',
            'added': added,
            'removed': removed,
            'modified': modified
        }
    
    @staticmethod
    def _format_bytes(size_bytes: float) -> str:
        if not size_bytes or size_bytes <= 0:
            return "0 B"
        units = ["B", "KB", "MB", "GB", "TB", "PB"]
        i = int(math.floor(math.log(size_bytes, 1024)))
        i = min(i, len(units) - 1)
        val = size_bytes / (1024 ** i)
        return f"{val:.1f} {units[i]}"

    def _query_tablet_report_db(self, db_path: str) -> Dict[str, Any]:
        """Query a SQLite tablet report database and return structured data."""
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Helper: in this schema, sst_size/wal_size may be empty strings
        SST = "CASE WHEN typeof(sst_size)='integer' THEN sst_size ELSE 0 END"
        WAL = "CASE WHEN typeof(wal_size)='integer' THEN wal_size ELSE 0 END"

        cur = conn.cursor()
        fmt = self._format_bytes
        try:
            # --- Cluster info ---
            cur.execute("SELECT * FROM cluster ORDER BY type, zone, ip")
            cluster_rows = [dict(r) for r in cur.fetchall()]
            tservers = [r for r in cluster_rows if r['type'] == 'TSERVER']
            masters = [r for r in cluster_rows if r['type'] == 'MASTER']

            # --- Version info ---
            try:
                cur.execute("SELECT * FROM version_info")
                version_row = cur.fetchone()
                version_info = dict(version_row) if version_row else {}
            except Exception:
                version_info = {}

            # --- Summary counts ---
            cur.execute("SELECT count(*) as cnt FROM tablet")
            total_tablets = cur.fetchone()['cnt']
            cur.execute("SELECT count(DISTINCT tablet_uuid) as cnt FROM tablet")
            unique_tablets = cur.fetchone()['cnt']
            cur.execute("SELECT count(DISTINCT table_name) as cnt FROM tablet")
            total_tables = cur.fetchone()['cnt']
            cur.execute("SELECT count(DISTINCT namespace) as cnt FROM tablet")
            total_namespaces = cur.fetchone()['cnt']

            # --- Total sizes ---
            cur.execute(f"""
                SELECT COALESCE(sum({SST}),0) as total_sst,
                       COALESCE(sum({WAL}),0) as total_wal
                FROM tablet
            """)
            totals = cur.fetchone()
            total_sst = totals['total_sst']
            total_wal = totals['total_wal']

            # --- Size by node ---
            cur.execute(f"""
                SELECT c.ip, c.zone, c.region, c.uuid as node_uuid,
                       count(*) as tablet_count,
                       COALESCE(sum({SST}),0) as sst_size,
                       COALESCE(sum({WAL}),0) as wal_size,
                       sum(CASE WHEN t.node_uuid = t.leader AND t.lease_status='HAS_LEASE' THEN 1 ELSE 0 END) as leaders
                FROM tablet t
                JOIN cluster c ON c.uuid = t.node_uuid AND c.type='TSERVER'
                GROUP BY c.ip, c.zone, c.region, c.uuid
                ORDER BY c.region, c.zone, c.ip
            """)
            node_sizes = []
            for r in cur.fetchall():
                row = dict(r)
                row['total_size'] = row['sst_size'] + row['wal_size']
                row['sst_human'] = fmt(row['sst_size'])
                row['wal_human'] = fmt(row['wal_size'])
                row['total_human'] = fmt(row['total_size'])
                node_sizes.append(row)

            # --- Size by zone ---
            cur.execute(f"""
                SELECT c.zone, c.region,
                       count(DISTINCT c.uuid) as tserver_count,
                       count(*) as tablet_count,
                       COALESCE(sum({SST}),0) as sst_size,
                       COALESCE(sum({WAL}),0) as wal_size,
                       sum(CASE WHEN t.node_uuid = t.leader AND t.lease_status='HAS_LEASE' THEN 1 ELSE 0 END) as leaders
                FROM tablet t
                JOIN cluster c ON c.uuid = t.node_uuid AND c.type='TSERVER'
                GROUP BY c.zone, c.region
                ORDER BY c.region, c.zone
            """)
            zone_sizes = []
            for r in cur.fetchall():
                row = dict(r)
                row['total_size'] = row['sst_size'] + row['wal_size']
                row['sst_human'] = fmt(row['sst_size'])
                row['wal_human'] = fmt(row['wal_size'])
                row['total_human'] = fmt(row['total_size'])
                zone_sizes.append(row)

            # --- Size by namespace ---
            cur.execute(f"""
                SELECT namespace,
                       count(DISTINCT table_name) as table_count,
                       count(DISTINCT tablet_uuid) as tablet_count,
                       COALESCE(sum({SST}),0) as sst_size,
                       COALESCE(sum({WAL}),0) as wal_size
                FROM tablet
                WHERE node_uuid = leader
                GROUP BY namespace
                ORDER BY sst_size DESC
            """)
            namespace_sizes = []
            for r in cur.fetchall():
                row = dict(r)
                row['total_size'] = row['sst_size'] + row['wal_size']
                row['sst_human'] = fmt(row['sst_size'])
                row['wal_human'] = fmt(row['wal_size'])
                row['total_human'] = fmt(row['total_size'])
                namespace_sizes.append(row)

            # --- Table sizes (top 50 by SST) ---
            cur.execute("""
                SELECT NAMESPACE as namespace, TABLENAME as tablename,
                       UNIQ_TABLET_COUNT as uniq_tablet_count,
                       TOT_TABLET_COUNT as tot_tablet_count,
                       SST_TOT_BYTES as sst_tot_bytes,
                       WAL_TOT_BYTES as wal_tot_bytes,
                       SST_TOT_HUMAN as sst_tot_human,
                       WAL_TOT_HUMAN as wal_tot_human,
                       SST_RF1_HUMAN as sst_rf1_human,
                       TOT_HUMAN as tot_human
                FROM tableinfo
                ORDER BY SST_TOT_BYTES DESC
                LIMIT 50
            """)
            table_sizes = [dict(r) for r in cur.fetchall()]

            # --- Tablet size distribution (SST sizes of leader tablets) ---
            cur.execute(f"""
                SELECT {SST} as sst_val FROM tablet
                WHERE lease_status='HAS_LEASE' AND typeof(sst_size)='integer' AND sst_size > 0
                ORDER BY sst_val
            """)
            tablet_sst_values = [r['sst_val'] for r in cur.fetchall()]

            # SST size buckets for distribution
            buckets = [
                (0, 1*1024*1024*1024, '<1 GB'),
                (1*1024*1024*1024, 2*1024*1024*1024, '1-2 GB'),
                (2*1024*1024*1024, 5*1024*1024*1024, '2-5 GB'),
                (5*1024*1024*1024, 10*1024*1024*1024, '5-10 GB'),
                (10*1024*1024*1024, 20*1024*1024*1024, '10-20 GB'),
                (20*1024*1024*1024, 50*1024*1024*1024, '20-50 GB'),
                (50*1024*1024*1024, float('inf'), '>50 GB'),
            ]
            size_distribution = []
            for lo, hi, label in buckets:
                cnt = sum(1 for v in tablet_sst_values if lo <= v < hi)
                if cnt > 0:
                    size_distribution.append({'label': label, 'count': cnt})

            # --- Unbalanced tables ---
            try:
                cur.execute("""
                    SELECT namespace, table_name, total_tablet_count,
                           unique_tablet_count, nodes, large_tablet,
                           min_tablet_mb, max_tablet_mb
                    FROM unbalanced_tables
                    LIMIT 100
                """)
                unbalanced_tables = [dict(r) for r in cur.fetchall()]
            except Exception:
                unbalanced_tables = []

            # --- Skew analysis: per-table size stats for top tables ---
            cur.execute(f"""
                SELECT t2.namespace, t2.table_name,
                       count(*) as replica_count,
                       min({SST}) as min_sst,
                       max({SST}) as max_sst,
                       avg({SST}) as avg_sst,
                       CASE WHEN min({SST}) > 0
                            THEN round(max({SST})*1.0/min({SST}), 1)
                            ELSE NULL END as skew_ratio
                FROM tablet t2
                WHERE t2.lease_status='HAS_LEASE' AND typeof(t2.sst_size)='integer' AND t2.sst_size > 0
                GROUP BY t2.namespace, t2.table_name
                HAVING count(*) > 1
                ORDER BY skew_ratio DESC
                LIMIT 50
            """)
            skew_analysis = []
            for r in cur.fetchall():
                row = dict(r)
                row['min_sst_human'] = fmt(row['min_sst'])
                row['max_sst_human'] = fmt(row['max_sst'])
                row['avg_sst_human'] = fmt(row['avg_sst'])
                skew_analysis.append(row)

            # --- Tablets per node ---
            cur.execute("""
                SELECT c.ip, c.zone, c.region,
                       count(*) as tablet_count,
                       sum(CASE WHEN t.status='TABLET_DATA_COPYING' THEN 1 ELSE 0 END) as copying,
                       sum(CASE WHEN t.state='TABLET_DATA_TOMBSTONED' THEN 1 ELSE 0 END) as tombstoned,
                       sum(CASE WHEN t.node_uuid = t.leader AND t.lease_status='HAS_LEASE' THEN 1 ELSE 0 END) as leaders
                FROM tablet t
                JOIN cluster c ON c.uuid = t.node_uuid AND c.type='TSERVER'
                GROUP BY c.ip, c.zone, c.region
                ORDER BY c.region, c.zone, c.ip
            """)
            tablets_per_node = [dict(r) for r in cur.fetchall()]

            # --- Replica summary ---
            try:
                cur.execute("SELECT * FROM tablet_replica_summary ORDER BY replicas")
                replica_summary = [dict(r) for r in cur.fetchall()]
            except Exception:
                replica_summary = []

            # --- Leaderless tablets ---
            try:
                cur.execute("SELECT count(*) as cnt FROM leaderless")
                leaderless_count = cur.fetchone()['cnt']
            except Exception:
                leaderless_count = 0

            # --- Region zone tablets ---
            try:
                cur.execute("SELECT * FROM region_zone_tablets")
                region_zone_tablets = [dict(r) for r in cur.fetchall()]
            except Exception:
                region_zone_tablets = []

            # --- Per-node size variance (for imbalance gauge) ---
            node_total_sizes = [n['total_size'] for n in node_sizes]
            if node_total_sizes:
                avg_node_size = sum(node_total_sizes) / len(node_total_sizes)
                max_node_size = max(node_total_sizes)
                min_node_size = min(node_total_sizes)
                node_skew_pct = round(((max_node_size - min_node_size) / avg_node_size) * 100, 1) if avg_node_size > 0 else 0
            else:
                avg_node_size = 0
                max_node_size = 0
                min_node_size = 0
                node_skew_pct = 0

            # --- Per-table size for treemap (leader tablets, top 200 tables) ---
            cur.execute(f"""
                SELECT namespace, table_name,
                       COALESCE(sum({SST}),0) as sst_size,
                       COALESCE(sum({WAL}),0) as wal_size,
                       count(*) as tablet_count
                FROM tablet
                WHERE lease_status='HAS_LEASE'
                GROUP BY namespace, table_name
                ORDER BY sst_size DESC
                LIMIT 200
            """)
            table_treemap = []
            for r in cur.fetchall():
                row = dict(r)
                row['total_size'] = row['sst_size'] + row['wal_size']
                row['sst_human'] = fmt(row['sst_size'])
                table_treemap.append(row)

            # ─── NEW: Per-table per-node SST (for heatmap + waterfall) ───
            tserver_count = len(tservers)
            cur.execute(f"""
                SELECT c.ip as node_ip,
                       t.namespace || '.' || t.table_name as full_table,
                       t.namespace,
                       t.table_name,
                       COALESCE(sum({SST}),0) as sst
                FROM tablet t
                JOIN cluster c ON c.uuid = t.node_uuid AND c.type='TSERVER'
                GROUP BY c.ip, t.namespace, t.table_name
                HAVING sst > 0
            """)
            table_node_raw = [dict(r) for r in cur.fetchall()]

            # Build per-table aggregated stats for heatmap
            from collections import defaultdict
            table_node_map = defaultdict(dict)
            table_totals = defaultdict(int)
            for row in table_node_raw:
                table_node_map[row['full_table']][row['node_ip']] = row['sst']
                table_totals[row['full_table']] += row['sst']

            # Node comparison waterfall: gap between heaviest and lightest node, per table
            node_ip_list = [n['ip'] for n in node_sizes]
            if len(node_sizes) >= 2:
                sorted_nodes = sorted(node_sizes, key=lambda n: n['total_size'], reverse=True)
                heavy_ip = sorted_nodes[0]['ip']
                light_ip = sorted_nodes[-1]['ip']
                heavy_total = sorted_nodes[0]['total_size']
                light_total = sorted_nodes[-1]['total_size']

                waterfall_items = []
                for tbl, per_node in table_node_map.items():
                    h_sst = per_node.get(heavy_ip, 0)
                    l_sst = per_node.get(light_ip, 0)
                    gap = h_sst - l_sst
                    if gap > 0:
                        waterfall_items.append({'table': tbl, 'gap': gap, 'gap_human': fmt(gap),
                                                'heavy_sst': h_sst, 'heavy_human': fmt(h_sst),
                                                'light_sst': l_sst, 'light_human': fmt(l_sst)})
                waterfall_items.sort(key=lambda x: x['gap'], reverse=True)
                cumulative = 0
                for item in waterfall_items:
                    cumulative += item['gap']
                    item['cumulative'] = cumulative
                    item['cumulative_human'] = fmt(cumulative)
                waterfall = {
                    'heavy_node': heavy_ip,
                    'light_node': light_ip,
                    'heavy_total': heavy_total,
                    'light_total': light_total,
                    'heavy_total_human': fmt(heavy_total),
                    'light_total_human': fmt(light_total),
                    'total_gap': heavy_total - light_total,
                    'total_gap_human': fmt(heavy_total - light_total),
                    'items': waterfall_items[:30],
                }
            else:
                waterfall = None

            # Per-table per-node heatmap: top 25 tables by total size
            top_tables_for_heatmap = sorted(table_totals.keys(), key=lambda t: table_totals[t], reverse=True)[:25]
            heatmap_data = []
            for tbl in top_tables_for_heatmap:
                per_node = table_node_map[tbl]
                node_values = [per_node.get(ip, 0) for ip in node_ip_list]
                avg_val = sum(node_values) / len(node_values) if node_values else 0
                max_val = max(node_values) if node_values else 0
                min_val = min(node_values) if node_values else 0
                spread = max_val - min_val
                row_total = table_totals[tbl]
                heatmap_data.append({
                    'table': tbl,
                    'total': row_total,
                    'total_human': fmt(row_total),
                    'per_node': {ip: per_node.get(ip, 0) for ip in node_ip_list},
                    'per_node_human': {ip: fmt(per_node.get(ip, 0)) for ip in node_ip_list},
                    'per_node_pct': {ip: round(per_node.get(ip, 0) / row_total * 100, 1) if row_total > 0 else 0 for ip in node_ip_list},
                    'max_node_val': max_val,
                    'spread': spread,
                    'spread_human': fmt(spread),
                    'node_count': sum(1 for ip in node_ip_list if per_node.get(ip, 0) > 0),
                })

            # ─── NEW: Split recommendations from large_tables view ───
            split_recommendations = []
            try:
                cur.execute("""
                    SELECT NAMESPACE as namespace, TABLENAME as tablename,
                           uniq_tablets, sst_RF1_mb, tablet_size_mb,
                           recommended_tablets, repl_factor, wal_RF1_mb
                    FROM large_tables
                    LIMIT 30
                """)
                for r in cur.fetchall():
                    row = dict(r)
                    row['sst_rf1_human'] = fmt(row['sst_RF1_mb'] * 1024 * 1024)
                    row['tablet_size_human'] = fmt(row['tablet_size_mb'] * 1024 * 1024)
                    row['recommended_tablets'] = int(row['recommended_tablets']) if row['recommended_tablets'] else row['uniq_tablets']
                    split_recommendations.append(row)
            except Exception:
                pass

            # ─── NEW: Auto-split phase detection ───
            unique_shards_per_node = unique_tablets / tserver_count if tserver_count > 0 else 0
            total_shards_per_node = total_tablets / tserver_count if tserver_count > 0 else 0
            if unique_shards_per_node < 1:
                auto_split_phase = 'low'
                split_threshold = 128 * 1024 * 1024
            elif total_shards_per_node < 24:
                auto_split_phase = 'high'
                split_threshold = 10 * 1024 * 1024 * 1024
            else:
                auto_split_phase = 'final'
                split_threshold = 100 * 1024 * 1024 * 1024

            # Count leader tablets in each threshold bucket
            above_threshold = sum(1 for v in tablet_sst_values if v >= split_threshold)
            below_threshold_large = sum(1 for v in tablet_sst_values if v >= 10 * 1024 * 1024 * 1024 and v < split_threshold)

            auto_split_info = {
                'phase': auto_split_phase,
                'shards_per_node': round(total_shards_per_node, 1),
                'unique_shards_per_node': round(unique_shards_per_node, 1),
                'threshold': split_threshold,
                'threshold_human': fmt(split_threshold),
                'above_threshold': above_threshold,
                'below_threshold_large': below_threshold_large,
                'total_leader_tablets': len(tablet_sst_values),
            }

            # ─── NEW: Leader data weight per node ───
            cur.execute(f"""
                SELECT c.ip,
                       COALESCE(sum({SST}),0) as leader_sst,
                       count(*) as leader_count
                FROM tablet t
                JOIN cluster c ON c.uuid = t.node_uuid AND c.type='TSERVER'
                WHERE t.node_uuid = t.leader AND t.lease_status='HAS_LEASE'
                GROUP BY c.ip
                ORDER BY leader_sst DESC
            """)
            leader_weight = []
            for r in cur.fetchall():
                row = dict(r)
                row['leader_sst_human'] = fmt(row['leader_sst'])
                leader_weight.append(row)

            # ─── NEW: Tables not spread across all nodes ───
            partial_spread = []
            if tserver_count > 1:
                threshold_bytes = 1 * 1024 * 1024 * 1024  # only flag tables > 1 GB total
                for tbl in sorted(table_totals.keys(), key=lambda t: table_totals[t], reverse=True):
                    if table_totals[tbl] < threshold_bytes:
                        continue
                    per_node = table_node_map[tbl]
                    present_on = sum(1 for ip in node_ip_list if per_node.get(ip, 0) > 0)
                    if present_on < tserver_count:
                        parts = tbl.split('.', 1)
                        partial_spread.append({
                            'table': tbl,
                            'namespace': parts[0] if len(parts) > 1 else '',
                            'table_name': parts[1] if len(parts) > 1 else tbl,
                            'total': table_totals[tbl],
                            'total_human': fmt(table_totals[tbl]),
                            'nodes_present': present_on,
                            'nodes_total': tserver_count,
                            'absent_nodes': [ip for ip in node_ip_list if per_node.get(ip, 0) == 0],
                            'per_node_human': {ip: fmt(per_node.get(ip, 0)) for ip in node_ip_list if per_node.get(ip, 0) > 0},
                        })
                    if len(partial_spread) >= 30:
                        break

            return {
                'cluster': cluster_rows,
                'tservers': tservers,
                'masters': masters,
                'version_info': version_info,
                'summary': {
                    'tserver_count': len(tservers),
                    'master_count': len(masters),
                    'total_tablets': total_tablets,
                    'unique_tablets': unique_tablets,
                    'total_tables': total_tables,
                    'total_namespaces': total_namespaces,
                    'total_sst': total_sst,
                    'total_wal': total_wal,
                    'total_size': total_sst + total_wal,
                    'total_sst_human': fmt(total_sst),
                    'total_wal_human': fmt(total_wal),
                    'total_size_human': fmt(total_sst + total_wal),
                    'leaderless_count': leaderless_count,
                    'node_skew_pct': node_skew_pct,
                    'avg_node_size_human': fmt(avg_node_size),
                    'min_node_size_human': fmt(min_node_size),
                    'max_node_size_human': fmt(max_node_size),
                },
                'node_sizes': node_sizes,
                'zone_sizes': zone_sizes,
                'namespace_sizes': namespace_sizes,
                'table_sizes': table_sizes,
                'tablet_sst_values': tablet_sst_values,
                'size_distribution': size_distribution,
                'unbalanced_tables': unbalanced_tables,
                'skew_analysis': skew_analysis,
                'tablets_per_node': tablets_per_node,
                'replica_summary': replica_summary,
                'region_zone_tablets': region_zone_tablets,
                'table_treemap': table_treemap,
                'waterfall': waterfall,
                'table_node_sst': {tbl: dict(nodes) for tbl, nodes in table_node_map.items()},
                'heatmap': heatmap_data,
                'heatmap_nodes': node_ip_list,
                'split_recommendations': split_recommendations,
                'auto_split_info': auto_split_info,
                'leader_weight': leader_weight,
                'partial_spread': partial_spread,
            }
        finally:
            conn.close()

    def run(self, debug: bool = False, host: str = None, port: int = None) -> None:
        """Run the Flask application."""
        host = host or settings.server.host
        port = port or settings.server.port
        
        self.logger.info(f"Starting web server on {host}:{port}")
        self.app.run(debug=debug, host=host, port=port)


def create_app() -> Flask:
    """Factory function to create Flask app."""
    web_app = LogAnalyzerWebApp()
    return web_app.app


# Create the Flask app instance for Gunicorn compatibility
app = create_app()


if __name__ == '__main__':
    # Set up logging
    setup_logging()
    
    # Create and run app
    web_app = LogAnalyzerWebApp()
    web_app.run(debug=True)