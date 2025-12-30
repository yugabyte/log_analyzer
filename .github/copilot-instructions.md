# Copilot Instructions for YugabyteDB Log Analyzer

## Project Overview
- This is a Python-based log analysis tool for YugabyteDB support bundles, with a Flask web interface and PostgreSQL backend.
- Follows clean architecture: config, models, services, utils, webserver, lib, tests.
- Main entry: `log_analyzer.py` (CLI) and `webserver/app.py` (web).

## Key Patterns & Conventions
- **Services** (`services/`): Each service encapsulates business logic (e.g., `analysis_service.py`, `database_service.py`). New features should be added as new service modules following this pattern.
- **Error Handling**: Use custom exceptions from `utils/exceptions.py`. Log errors with structured messages using `utils/logging_config.py`.
- **Type Hints**: All new code should use Python type hints for function signatures and data models.
- **Configuration**: Centralized in `config/settings.py`, `db_config.json`, `server_config.json`, and `log_conf.yml`. Always read config via provided helpers.
- **Database**: PostgreSQL schema defined in `schema.sql`. Use prepared statements and connection pooling (see `services/database_service.py`).
- **Testing**: Tests live in `tests/`. Use `pytest` and follow the structure in `test_analysis_service.py`.
- **Web Interface**: Flask app in `webserver/app.py`, templates in `webserver/templates/`, static files in `webserver/static/`.
- **Legacy Code**: Some logic in `lib/` (e.g., `parquet_lib.py`). Prefer refactored service code for new features.

## Developer Workflows
- **Setup**: Use Python 3.8+, create a virtualenv, install with `pip install -r requirements.txt`.
- **Database**: Initialize with `psql -d <db> -f schema.sql`.
- **Run CLI**: `python log_analyzer.py -s <support_bundle.tar.gz>` (see README for options).
- **Run Web**: `python webserver/app.py` and browse to `http://localhost:5000`.
- **Testing**: Run `pytest` or `pytest --cov=.` for coverage.

## Integration Points
- **Support Bundle Parsing**: File handling via `services/file_processor.py`, pattern matching via `services/pattern_matcher.py`.
- **Tablet Report Parsing**: Add new service as `services/tablet_report_service.py`, using parsing logic from `tablet_report_parser.py` and database schema from `schema.sql`.
- **External Dependencies**: DuckDB for Parquet, PostgreSQL for persistence.

## Examples
- To add a new report type, create a service in `services/`, add database tables to `schema.sql`, and update the web interface in `webserver/app.py` and templates.
- For error handling, raise `AnalysisError` or other custom exceptions and log via `logging_config.py`.

## References
- See `README.md` for architecture, setup, and usage details.
- See `services/` for service structure and style.
- See `tablet_report_parser.py` for parsing logic examples.

---
If any section is unclear or missing, please provide feedback so instructions can be improved for future agents.
