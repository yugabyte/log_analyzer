from flask import Flask, render_template, request, redirect, url_for, jsonify, send_from_directory
import json
from collections import defaultdict
import os

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'

if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

# In-memory storage for uploaded data
uploaded_data = {}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    file = request.files.get('file')
    if not file or not file.filename.endswith('.json'):
        return 'Invalid file', 400
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(filepath)
    with open(filepath, 'r') as f:
        data = json.load(f)
    uploaded_data['data'] = data
    return jsonify({'status': 'success'})

@app.route('/data')
def get_data():
    if 'data' not in uploaded_data:
        return jsonify({'error': 'No data uploaded'}), 404
    return jsonify(uploaded_data['data'])

@app.route('/img/<path:filename>')
def serve_img(filename):
    return send_from_directory(os.path.join(app.root_path, 'img'), filename)

if __name__ == '__main__':
    app.run(debug=True)
