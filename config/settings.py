"""
Configuration settings for the Log Analyzer application.

This module contains all configuration constants, settings, and environment
variables used throughout the application.
"""

import os
from typing import Dict, Any
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass
class ServerConfig:
    """Web server configuration settings."""
    host: str
    port: int


@dataclass
class AnalysisConfig:
    """Log analysis configuration settings."""
    default_parallel_threads: int = 5
    default_time_range_days: int = 7
    max_file_size_mb: int = 100
    supported_log_types: Dict[str, str] = None
    supported_process_types: list = None
    
    def __post_init__(self):
        if self.supported_log_types is None:
            self.supported_log_types = {
                'pg': 'postgres',
                'ts': 'yb-tserver',
                'ms': 'yb-master',
                'ybc': 'yb-controller',
            }
        
        if self.supported_process_types is None:
            self.supported_process_types = ['postgres', 'tserver', 'controller', 'master']


class Settings:
    """Application settings manager."""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent.parent
        self.analysis_config = AnalysisConfig()
        self._load_database_config()
        self._load_server_config()
    
    def _load_database_config(self) -> None:
        """Load database configuration from JSON file."""
        db_config_path = self.base_dir / "db_config.json"
        if db_config_path.exists():
            import json
            with open(db_config_path) as f:
                db_data = json.load(f)
            self.database = DatabaseConfig(**db_data)
            print(f"✅ Database config loaded from {db_config_path}")
        else:
            # Default database configuration
            self.database = DatabaseConfig(
                host="localhost",
                port=5432,
                dbname="log_analyzer",
                user="postgres",
                password="password"
            )
            print(f"⚠️  Using default database config (file not found: {db_config_path})")
    
    def _load_server_config(self) -> None:
        """Load server configuration from JSON file."""
        server_config_path = self.base_dir / "server_config.json"
        if server_config_path.exists():
            import json
            with open(server_config_path) as f:
                server_data = json.load(f)
            self.server = ServerConfig(**server_data)
            print(f"✅ Server config loaded from {server_config_path}")
        else:
            # Default server configuration
            self.server = ServerConfig(host="127.0.0.1", port=5000)
            print(f"⚠️  Using default server config (file not found: {server_config_path})")
    
    @property
    def log_conf_path(self) -> Path:
        """Path to the log configuration YAML file."""
        return self.base_dir / "log_conf.yml"
    
    @property
    def uploads_dir(self) -> Path:
        """Path to the uploads directory."""
        return self.base_dir / "webserver" / "uploads"
    
    @property
    def static_dir(self) -> Path:
        """Path to the static files directory."""
        return self.base_dir / "webserver" / "static"
    
    @property
    def templates_dir(self) -> Path:
        """Path to the templates directory."""
        return self.base_dir / "webserver" / "templates"


# Global settings instance
settings = Settings() 