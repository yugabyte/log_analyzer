"""
Data models for log metadata and analysis results.

This module defines the data structures used throughout the application
for representing log files, metadata, and analysis results.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path


@dataclass
class LogFileMetadata:
    """Metadata for a single log file."""
    
    file_path: Path
    node_name: str
    log_type: str
    sub_type: str
    start_time: datetime
    end_time: datetime
    
    def __post_init__(self):
        """Validate the metadata after initialization."""
        if not self.file_path.exists():
            raise ValueError(f"Log file does not exist: {self.file_path}")
        
        if self.start_time > self.end_time:
            raise ValueError("Start time cannot be after end time")


@dataclass
class LogMessageStats:
    """Statistics for a specific log message pattern."""
    
    pattern_name: str
    start_time: datetime
    end_time: datetime
    count: int
    histogram: Dict[str, int] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for JSON serialization."""
        return {
            "StartTime": self.start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "EndTime": self.end_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "count": self.count,
            "histogram": self.histogram
        }


@dataclass
class NodeAnalysisResult:
    """Analysis results for a single node."""
    
    node_name: str
    log_type: str
    log_messages: Dict[str, LogMessageStats] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for JSON serialization."""
        return {
            "node": self.node_name,
            "logType": self.log_type,
            "logMessages": {
                name: stats.to_dict() 
                for name, stats in self.log_messages.items()
            }
        }


@dataclass
class AnalysisReport:
    """Complete analysis report."""
    
    support_bundle_name: str
    nodes: Dict[str, Dict[str, NodeAnalysisResult]] = field(default_factory=dict)
    warnings: List[Dict[str, Any]] = field(default_factory=list)
    analysis_config: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format for JSON serialization."""
        return {
            "nodes": {
                node_name: {
                    log_type: result.to_dict()
                    for log_type, result in node_data.items()
                }
                for node_name, node_data in self.nodes.items()
            },
            "warnings": self.warnings,
            "analysis_config": self.analysis_config
        }


@dataclass
class SupportBundleInfo:
    """Information about a support bundle."""
    
    name: str
    directory: Path
    extracted_path: Optional[Path] = None
    log_files_metadata: Dict[str, Dict[str, Dict[str, Dict[str, LogFileMetadata]]]] = field(default_factory=dict)
    
    def get_log_files_count(self) -> int:
        """Get the total number of log files in the support bundle."""
        count = 0
        for node_data in self.log_files_metadata.values():
            for log_type_data in node_data.values():
                for sub_type_data in log_type_data.values():
                    count += len(sub_type_data)
        return count
    
    def get_nodes(self) -> List[str]:
        """Get list of node names in the support bundle."""
        return list(self.log_files_metadata.keys())
    
    def get_log_types(self) -> List[str]:
        """Get list of log types in the support bundle."""
        log_types = set()
        for node_data in self.log_files_metadata.values():
            for log_type in node_data.keys():
                log_types.add(log_type)
        return list(log_types)


@dataclass
class AnalysisConfig:
    """Configuration for log analysis."""
    
    start_time: datetime
    end_time: datetime
    parallel_threads: int = 5
    histogram_mode: Optional[List[str]] = None
    node_filter: Optional[List[str]] = None
    log_type_filter: Optional[List[str]] = None
    
    def validate(self) -> None:
        """Validate the analysis configuration."""
        if self.start_time > self.end_time:
            raise ValueError("Start time cannot be after end time")
        
        if self.parallel_threads < 1:
            raise ValueError("Parallel threads must be at least 1")
        
        if self.parallel_threads > 20:
            raise ValueError("Parallel threads cannot exceed 20") 