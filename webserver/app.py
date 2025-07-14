import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from flask import Flask, render_template, request, redirect, url_for, jsonify, send_from_directory
import json
from collections import defaultdict
import psycopg2
from datetime import datetime, timedelta
from patterns_lib import solutions

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'

if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

# In-memory storage for uploaded data
uploaded_data = {}

# Helper to load DB config

def load_db_config():
    with open(os.path.join(os.path.dirname(__file__), '..', 'db_config.json')) as f:
        return json.load(f)

@app.route('/')
def index():
    # Fetch paginated reports from DB
    page = int(request.args.get('page', 1))
    per_page = 10
    offset = (page - 1) * per_page
    try:
        db_config = load_db_config()
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM public.log_analyzer_reports
        """)
        total_reports = cur.fetchone()[0]
        total_pages = (total_reports + per_page - 1) // per_page
        cur.execute("""
            SELECT r.id, r.support_bundle_name, h.cluster_name, h.organization, h.case_id, r.created_at
            FROM public.log_analyzer_reports r
            LEFT JOIN public.support_bundle_header h ON r.support_bundle_name = h.support_bundle
            ORDER BY r.created_at DESC LIMIT %s OFFSET %s
        """, (per_page, offset))
        reports = [
            {
                'id': str(row[0]),
                'support_bundle_name': row[1],
                'universe_name': row[2] or '',
                'organization_name': row[3] or '',
                'case_id': row[4],
                'created_at': row[5].strftime('%Y-%m-%d %H:%M')
            }
            for row in cur.fetchall()
        ]
        cur.close()
        conn.close()
    except Exception as e:
        reports = []
        total_pages = 1
    print('DEBUG: reports fetched for index:', reports)
    return render_template('index.html', reports=reports, page=page, total_pages=total_pages)

@app.route('/upload', methods=['POST'])
def upload():
    # Accepts JSON payload with keys: universe_name, ticket, json_report
    try:
        data = request.get_json()
        universe_name = data['universe_name']
        ticket = data.get('ticket', '')
        json_report = json.dumps(data['json_report'])
        db_config = load_db_config()
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO log_analyzer.reports (universe_name, ticket, json_report, created_at)
            VALUES (%s, %s, %s, NOW())
            RETURNING id
        """, (universe_name, ticket, json_report))
        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'status': 'success', 'id': new_id})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/data')
def get_data():
    if 'data' not in uploaded_data:
        return jsonify({'error': 'No data uploaded'}), 404
    return jsonify(uploaded_data['data'])

@app.route('/img/<path:filename>')
def serve_img(filename):
    return send_from_directory(os.path.join(app.root_path, 'img'), filename)

@app.route('/reports/<uuid>')
def report_page(uuid):
    # Inject solutions as a JS object for the frontend
    return render_template('reports.html', report_uuid=uuid, log_solutions_map=solutions)

@app.route('/reports/<int:report_id>')
def report_json(report_id):
    # Return the report in JSON format from DB
    try:
        db_config = load_db_config()
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT json_report FROM log_analyzer.reports WHERE id = %s
        """, (report_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return jsonify(row[0])
        else:
            return jsonify({'error': 'Report not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Keep the old /reports/<uuid> JSON API as /api/reports/<uuid>
@app.route('/api/reports/<uuid>')
def get_report_api(uuid):
    try:
        db_config = load_db_config()
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT json_report FROM public.log_analyzer_reports WHERE id::text = %s
        """, (str(uuid),))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if row:
            return jsonify(row[0])
        else:
            return jsonify({'error': 'Report not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/histogram/<report_id>')
def histogram_api(report_id):
    """
    Query params:
      - interval: int (minutes, one of 1,5,15,30,60)
      - start: ISO8601 string (inclusive)
      - end: ISO8601 string (inclusive)
    """
    interval = int(request.args.get('interval', 1))
    start = request.args.get('start')
    end = request.args.get('end')
    try:
        db_config = load_db_config()
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        cur = conn.cursor()
        # Try both int and str for report_id
        try:
            cur.execute("""
                SELECT json_report FROM public.log_analyzer_reports WHERE id::text = %s
            """, (str(report_id),))
        except Exception:
            cur.execute("""
                SELECT json_report FROM public.log_analyzer_reports WHERE id = %s
            """, (report_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return jsonify({'error': 'Report not found'}), 404
        data = row[0]
        if isinstance(data, str):
            data = json.loads(data)
        # Filter and aggregate histogram
        def parse_time(s):
            return datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ')
        def format_time(dt):
            return dt.strftime('%Y-%m-%dT%H:%M:00Z')
        if start:
            start_dt = parse_time(start)
        else:
            start_dt = None
        if end:
            end_dt = parse_time(end)
        else:
            end_dt = None
        for node, node_data in data.get('nodes', {}).items():
            for proc, proc_data in node_data.items():
                for msg, msg_stats in proc_data.get('logMessages', {}).items():
                    hist = msg_stats.get('histogram', {})
                    # Filter by time range
                    filtered = {}
                    for k, v in hist.items():
                        t = parse_time(k)
                        if (not start_dt or t >= start_dt) and (not end_dt or t <= end_dt):
                            filtered[k] = v
                    # Aggregate by interval
                    if interval > 1:
                        agg = {}
                        for k, v in filtered.items():
                            t = parse_time(k)
                            bucket_minute = (t.minute // interval) * interval
                            bucket = t.replace(minute=bucket_minute, second=0, microsecond=0)
                            bucket_key = format_time(bucket)
                            agg[bucket_key] = agg.get(bucket_key, 0) + v
                        msg_stats['histogram'] = agg
                    else:
                        msg_stats['histogram'] = filtered
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/gflags/<uuid>')
def gflags_api(uuid):
    try:
        db_config = load_db_config()
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        cur = conn.cursor()
        # Get support_bundle_name for this report
        cur.execute("SELECT support_bundle_name FROM public.log_analyzer_reports WHERE id::text = %s", (str(uuid),))
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            return jsonify({'error': 'Report not found'}), 404
        support_bundle_name = row[0]
        # Query GFlags for this support_bundle, grouped by server_type and gflag
        cur.execute("""
            SELECT server_type, gflag, value
            FROM public.support_bundle_gflags
            WHERE support_bundle = %s
        """, (support_bundle_name,))
        gflags = {}
        for server_type, gflag, value in cur.fetchall():
            if server_type not in gflags:
                gflags[server_type] = {}
            gflags[server_type][gflag] = value
        cur.close()
        conn.close()
        return jsonify(gflags)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/related_reports/<uuid>')
def related_reports_api(uuid):
    try:
        db_config = load_db_config()
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        cur = conn.cursor()
        # Get support_bundle_name for this report
        cur.execute("SELECT support_bundle_name FROM public.log_analyzer_reports WHERE id::text = %s", (str(uuid),))
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            return jsonify({'error': 'Report not found'}), 404
        support_bundle_name = row[0]
        # Get cluster_uuid and organization for this report
        cur.execute("""
            SELECT h.cluster_uuid, h.organization
            FROM public.support_bundle_header h
            WHERE h.support_bundle = %s
        """, (support_bundle_name,))
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            return jsonify({'same_cluster': [], 'same_org': []})
        cluster_uuid, organization = row
        # Find all reports for the same cluster (excluding current)
        cur.execute("""
            SELECT r.id, r.support_bundle_name, h.cluster_name, h.organization, h.cluster_uuid, h.case_id, r.created_at
            FROM public.log_analyzer_reports r
            JOIN public.support_bundle_header h ON r.support_bundle_name = h.support_bundle
            WHERE h.cluster_uuid = %s
              AND r.id::text != %s
            ORDER BY r.created_at DESC LIMIT 20
        """, (str(cluster_uuid), str(uuid)))
        same_cluster = [
            {
                'id': str(r[0]),
                'support_bundle_name': r[1],
                'cluster_name': r[2],
                'organization': r[3],
                'cluster_uuid': str(r[4]),
                'case_id': r[5],
                'created_at': r[6].strftime('%Y-%m-%d %H:%M')
            }
            for r in cur.fetchall()
        ]
        # Find all reports for the same organization, but NOT in the same cluster (excluding current)
        cur.execute("""
            SELECT r.id, r.support_bundle_name, h.cluster_name, h.organization, h.cluster_uuid, h.case_id, r.created_at
            FROM public.log_analyzer_reports r
            JOIN public.support_bundle_header h ON r.support_bundle_name = h.support_bundle
            WHERE h.organization = %s
              AND h.cluster_uuid != %s
              AND r.id::text != %s
            ORDER BY r.created_at DESC LIMIT 20
        """, (organization, str(cluster_uuid), str(uuid)))
        same_org = [
            {
                'id': str(r[0]),
                'support_bundle_name': r[1],
                'cluster_name': r[2],
                'organization': r[3],
                'cluster_uuid': str(r[4]),
                'case_id': r[5],
                'created_at': r[6].strftime('%Y-%m-%d %H:%M')
            }
            for r in cur.fetchall()
        ]
        cur.close()
        conn.close()
        return jsonify({'same_cluster': same_cluster, 'same_org': same_org})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/search_reports')
def search_reports():
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify([])
    try:
        db_config = load_db_config()
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        cur = conn.cursor()
        # Search by id, support_bundle_name, cluster_name, organization, or case_id
        cur.execute("""
            SELECT r.id, r.support_bundle_name, h.cluster_name, h.organization, h.case_id, r.created_at
            FROM public.log_analyzer_reports r
            LEFT JOIN public.support_bundle_header h ON r.support_bundle_name = h.support_bundle
            WHERE r.id::text ILIKE %s
               OR r.support_bundle_name ILIKE %s
               OR h.cluster_name ILIKE %s
               OR h.organization ILIKE %s
               OR h.case_id::text ILIKE %s
            ORDER BY r.created_at DESC LIMIT 20
        """, tuple(['%' + query + '%'] * 5))
        reports = [
            {
                'id': str(row[0]),
                'support_bundle_name': row[1],
                'universe_name': row[2] or '',
                'organization_name': row[3] or '',
                'case_id': row[4],
                'created_at': row[5].strftime('%Y-%m-%d %H:%M')
            }
            for row in cur.fetchall()
        ]
        cur.close()
        conn.close()
        return jsonify({
            "reports": reports,
            "page": 1,
            "total_pages": 1
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/node_info/<uuid>')
def node_info_api(uuid):
    """
    Returns node info for the given report UUID, as a flat list of nodes.
    """
    try:
        db_config = load_db_config()
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        cur = conn.cursor()
        # Get support_bundle_name for this report
        cur.execute("SELECT support_bundle_name FROM public.log_analyzer_reports WHERE id::text = %s", (str(uuid),))
        row = cur.fetchone()
        if not row:
            cur.close()
            conn.close()
            return jsonify({'error': 'Report not found'}), 404
        support_bundle_name = row[0]
        # Query node info from the new view
        cur.execute("""
            SELECT node_name, state, is_master, is_tserver, cloud || '.' || region || '.' || az as placement, num_cores, mem_size_gb, volume_size_gb
            FROM public.view_support_bundle_yba_metadata_cluster_summary
            WHERE support_bundle = %s
        """, (support_bundle_name,))
        nodes = []
        for r in cur.fetchall():
            nodes.append({
                'node_name': r[0],
                'state': r[1],
                'is_master': r[2],
                'is_tserver': r[3],
                'placement': r[4],
                'num_cores': r[5],
                'mem_size_gb': float(r[6]) if r[6] is not None else None,
                'volume_size_gb': float(r[7]) if r[7] is not None else None
            })
        cur.close()
        conn.close()
        return jsonify({'nodes': nodes})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
