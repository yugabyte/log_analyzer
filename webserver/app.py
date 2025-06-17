from flask import Flask, render_template, request, redirect, url_for, jsonify, send_from_directory
import json
from collections import defaultdict
import os
import psycopg2

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

if __name__ == '__main__':
    app.run(debug=True)
