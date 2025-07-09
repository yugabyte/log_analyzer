# YugabyteDB Log Analyzer

YugabyteDB Log Analyzer is a tool for uploading, analyzing, and visualizing YugabyteDB log files. It provides a web interface to view reports, histograms, GFlags, and related diagnostic information.

## Features

- Upload YugabyteDB log bundles and generate analysis reports
- View detailed report data, histograms, and GFlags
- Search and filter reports by cluster, org, or case ID
- Explore related reports and log solutions

## Project Structure

- `log_analyzer.py` – Main log analysis logic
- `patterns_lib.py` – Log pattern matching library
- `lib/` – Utility modules for log analysis
- `webserver/app.py` – Flask web server for the UI and API
- `webserver/static/` – Frontend assets (JS, CSS)
- `webserver/templates/` – HTML templates

## Getting Started

### Prerequisites

- Python 3.10+
- Flask
- psycopg2 (for PostgreSQL database)

### Installation

1. Clone the repository:
   ```bash
   git clone <repo-url>
   cd log_analyzer
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Running the Web Server

```bash
cd webserver
python app.py
```

The server will start at `http://localhost:5000`.

### Uploading a Report

You can upload a report via the web UI or using the API:

```bash
curl -X POST -H "Content-Type: application/json" -d @report.json http://localhost:5000/upload
```

## API Reference

See [API_ENDPOINTS.md](API_ENDPOINTS.md) for a detailed list of API endpoints and example usage.

## Configuration

- `db_config.json` – Database configuration
- `log_conf.yml` – Log analysis configuration

## License

MIT License

---

For questions or contributions, please open an issue or pull request.
