"""
Refactored Flask web server for Log Analyzer.

This module provides a clean, maintainable web interface for viewing
log analysis reports with proper error handling and type hints.
"""

import sys
import os
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