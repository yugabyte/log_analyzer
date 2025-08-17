"""
Custom exceptions for the Log Analyzer application.

This module defines application-specific exceptions that provide
better error handling and debugging information.
"""

from typing import Optional, Any


class LogAnalyzerError(Exception):
    """Base exception for all Log Analyzer errors."""
    
    def __init__(self, message: str, details: Optional[Any] = None):
        super().__init__(message)
        self.message = message
        self.details = details


class ConfigurationError(LogAnalyzerError):
    """Raised when there's an issue with configuration files or settings."""
    pass


class DatabaseError(LogAnalyzerError):
    """Raised when there's an issue with database operations."""
    pass


class FileProcessingError(LogAnalyzerError):
    """Raised when there's an issue processing log files."""
    pass


class SupportBundleError(LogAnalyzerError):
    """Raised when there's an issue with support bundle processing."""
    pass


class AnalysisError(LogAnalyzerError):
    """Raised when there's an issue during log analysis."""
    pass


class ValidationError(LogAnalyzerError):
    """Raised when input validation fails."""
    pass


class ReportGenerationError(LogAnalyzerError):
    """Raised when there's an issue generating reports."""
    pass 