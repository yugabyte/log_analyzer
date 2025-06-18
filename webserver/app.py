from flask import Flask, render_template, request, redirect, url_for, jsonify, send_from_directory
import json
from collections import defaultdict
import os
import psycopg2
from datetime import datetime, timedelta

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'

if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

# In-memory storage for uploaded data
uploaded_data = {}

@app.route('/')
def index():
    # Fetch recent reports from DB
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="log_analyzer_user",
            password="changeme",
            host="localhost",
            port=5432
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT id, universe_name, ticket, created_at FROM log_analyzer.reports ORDER BY created_at DESC LIMIT 10
        """)
        reports = [
            {
                'id': row[0],
                'universe_name': row[1],
                'ticket': row[2],
                'created_at': row[3].strftime('%Y-%m-%d %H:%M')
            }
            for row in cur.fetchall()
        ]
        cur.close()
        conn.close()
    except Exception as e:
        reports = []
    return render_template('index.html', reports=reports)

@app.route('/upload', methods=['POST'])
def upload():
    # Accepts JSON payload with keys: universe_name, ticket, json_report
    try:
        data = request.get_json()
        universe_name = data['universe_name']
        ticket = data.get('ticket', '')
        json_report = json.dumps(data['json_report'])
        conn = psycopg2.connect(
            dbname="postgres",
            user="log_analyzer_user",
            password="changeme",
            host="localhost",
            port=5432
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
    return render_template('reports.html', report_uuid=uuid)

@app.route('/reports/<int:report_id>')
def report_json(report_id):
    # Return the report in JSON format from DB
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="log_analyzer_user",
            password="changeme",
            host="localhost",
            port=5432
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
        conn = psycopg2.connect(
            dbname="postgres",
            user="log_analyzer_user",
            password="changeme",
            host="localhost",
            port=5432
        )
        cur = conn.cursor()
        cur.execute("""
            SELECT json_report FROM log_analyzer.reports WHERE id = %s
        """, (uuid,))
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
        conn = psycopg2.connect(
            dbname="postgres",
            user="log_analyzer_user",
            password="changeme",
            host="localhost",
            port=5432
        )
        cur = conn.cursor()
        # Try both int and str for report_id
        try:
            cur.execute("""
                SELECT json_report FROM log_analyzer.reports WHERE id = %s
            """, (int(report_id),))
        except Exception:
            cur.execute("""
                SELECT json_report FROM log_analyzer.reports WHERE id::text = %s
            """, (str(report_id),))
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

if __name__ == '__main__':
    app.run(debug=True)
