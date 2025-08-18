# Log Analyzer for YugabyteDB

A modern, maintainable, and efficient log analysis tool for YugabyteDB support bundles. This version follows best practices including proper separation of concerns, comprehensive error handling, type hints, and clean architecture.

## 🚀 Features

- **Support Bundle Analysis**: Extract and analyze YugabyteDB support bundles
- **Parquet File Analysis**: Process log data stored in Parquet format
- **Pattern Matching**: Configurable regex patterns for log message analysis
- **Parallel Processing**: Multi-threaded analysis for improved performance
- **Web Interface**: Flask-based web server for viewing reports
- **Database Storage**: PostgreSQL integration for report persistence
- **Comprehensive Logging**: Structured logging with colorized output
- **Type Safety**: Full type hints throughout the codebase

## 📋 Requirements

- Python 3.8+
- PostgreSQL 12+
- DuckDB (for Parquet analysis)

## 🛠️ Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd log_analyzer
   ```

2. **Create a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up configuration**:
   ```bash
   # Copy example configuration files
   cp db_config.json.example db_config.json
   cp server_config.json.example server_config.json
   
   # Edit configuration files with your settings
   nano db_config.json
   nano server_config.json
   ```

5. **Set up database**:
   ```sql
   -- Run the schema.sql file in your PostgreSQL database
   psql -d your_database -f schema.sql
   ```

## 🏗️ Architecture

The codebase follows a clean architecture pattern with clear separation of concerns:

```
log_analyzer/
├── config/                 # Configuration management
│   └── settings.py        # Centralized settings
├── models/                # Data models
│   └── log_metadata.py    # Type-safe data structures
├── services/              # Business logic services
│   ├── analysis_service.py    # Main analysis orchestration
│   ├── database_service.py    # Database operations
│   ├── file_processor.py      # File handling
│   ├── pattern_matcher.py     # Pattern matching
│   └── parquet_service.py     # Parquet analysis
├── utils/                 # Utilities and helpers
│   ├── exceptions.py      # Custom exceptions
│   └── logging_config.py  # Logging configuration
├── webserver/             # Web interface
│   ├── app.py             # Flask app
│   ├── static/            # Static files
│   └── templates/         # HTML templates
├── lib/                   # Legacy library modules
├── tests/                 # Test suite
├── log_analyzer.py        # Main application
└── requirements.txt       # Dependencies
```

## 🚀 Usage

### Command Line Interface

#### Analyze Support Bundle
```bash
# Basic analysis
python log_analyzer.py -s support_bundle.tar.gz

# With custom time range
python log_analyzer.py -s support_bundle.tar.gz \
  -t "1231 10:30" -T "1231 23:59"

# With node and log type filters
python log_analyzer.py -s support_bundle.tar.gz \
  -n "n1,n2" --types "pg,ts"

# With custom patterns
python log_analyzer.py -s support_bundle.tar.gz \
  --histogram-mode "error1,error2,error3"

# Parallel processing
python log_analyzer.py -s support_bundle.tar.gz \
  -p 8
```

#### Analyze Parquet Files
```bash
# Analyze Parquet directory
python log_analyzer.py --parquet_files /path/to/parquet/dir

# With custom patterns
python log_analyzer.py --parquet_files /path/to/parquet/dir \
  --histogram-mode "error1,error2,error3"

# Parallel processing
python log_analyzer.py --parquet_files /path/to/parquet/dir \
  -p 8
```

### Web Interface

1. **Start the web server**:
```bash
   python webserver/app.py
```

2. **Access the web interface**:
   Open your browser and navigate to `http://localhost:5000`

3. **View reports**:
   - Browse all reports on the main page
   - Click on any report to view detailed analysis
   ```bash
   python webserver/app.py
   ```

2. **Access the web interface**:
```bash
   python webserver/app.py
   Open your browser and navigate to `http://localhost:5000`

3. **View reports**:
   - Browse all reports on the main page
   - Click on any report to view detailed analysis
   - Use the search functionality to find specific reports

## ⚙️ Configuration

### Database Configuration (`db_config.json`)
```json
{
  "host": "localhost",
  "port": 5432,
  "dbname": "log_analyzer",
  "user": "postgres",
  "password": "your_password"
}
```

### Server Configuration (`server_config.json`)
```json
{
  "host": "127.0.0.1",
  "port": 5000
}
```

### Log Configuration (`log_conf.yml`)
```yaml
universe:
  log_messages:
    - name: "tablet_not_found"
      pattern: "Tablet.*not found"
      solution: "Check tablet distribution and replication"
    - name: "leader_not_ready"
      pattern: "Leader.*not ready"
      solution: "Check leader election and consensus"

pg:
  log_messages:
    - name: "connection_error"
      pattern: "connection.*failed"
      solution: "Check network connectivity and firewall rules"
```

## 🧪 Testing

Run the test suite:
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=.

# Run specific test file
pytest tests/test_analysis_service.py
```

## 🔧 Development

### Adding New Features

1. **Create new service**:
   ```python
   # services/new_service.py
   from utils.exceptions import AnalysisError
   
   class NewService:
       def __init__(self):
           pass
       
       def process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
           # Implementation
           pass
   ```

2. **Add tests**:
   ```python
   # tests/test_new_service.py
   import pytest
   from services.new_service import NewService
   
   def test_new_service():
       service = NewService()
       result = service.process_data({"test": "data"})
       assert result is not None
   ```

## 📊 Performance

The version includes several performance improvements:

- **Parallel Processing**: Multi-threaded analysis for large support bundles
- **Efficient File Handling**: Streaming file processing to reduce memory usage
- **Database Optimization**: Prepared statements and connection pooling
- **Caching**: Pattern compilation caching for repeated analysis

## 🔒 Error Handling

The version includes comprehensive error handling:

- **Custom Exceptions**: Domain-specific exception classes
- **Graceful Degradation**: Continue processing even if some files fail
- **Detailed Logging**: Structured logging with different levels
- **User-Friendly Messages**: Clear error messages for end users

## 📈 Monitoring

The application includes built-in monitoring capabilities:

- **Progress Tracking**: Real-time progress bars for long-running operations
- **Performance Metrics**: Timing information for different analysis phases
- **Resource Usage**: Memory and CPU usage monitoring
- **Error Tracking**: Detailed error logs with stack traces

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-feature`
3. Make your changes following the coding standards
4. Add tests for new functionality
5. Run the test suite: `pytest`
6. Submit a pull request

### Coding Standards

- Use type hints throughout
- Follow PEP 8 style guidelines
- Write comprehensive docstrings
- Add tests for new functionality
- Use meaningful variable and function names

## 🔄 Migration from Original Version

The version maintains backward compatibility with the original:

1. **Same Command Line Interface**: All original arguments are supported
2. **Same Output Format**: Reports are generated in the same JSON format
3. **Same Web Interface**: The web UI remains functionally identical
4. **Configuration Files**: Existing configuration files work without changes

### Key Improvements

- **Better Error Handling**: More informative error messages
- **Improved Performance**: Faster processing with parallel execution
- **Enhanced Logging**: Better visibility into analysis progress
- **Type Safety**: Reduced bugs through static type checking
- **Maintainability**: Cleaner code structure for easier maintenance 