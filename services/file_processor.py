"""
File processing service for handling log files and support bundles.

This module provides services for extracting, reading, and processing
log files from support bundles with proper error handling.
"""

import gzip
import tarfile
import os
from pathlib import Path
from typing import Iterator, Optional, List, Dict, Any
from datetime import datetime
import logging

from utils.exceptions import FileProcessingError, SupportBundleError
from models.log_metadata import LogFileMetadata, SupportBundleInfo
from config.settings import settings


logger = logging.getLogger(__name__)


class FileProcessor:
    """Service for processing log files and support bundles."""
    
    def __init__(self):
        self.supported_extensions = {'.log', '.gz', '.txt'}
        self.archive_extensions = {'.tar.gz', '.tgz'}
    
    def extract_support_bundle(self, bundle_path: Path) -> Path:
        """
        Extract a support bundle archive.
        
        Args:
            bundle_path: Path to the support bundle archive
            
        Returns:
            Path to the extracted directory
            
        Raises:
            SupportBundleError: If extraction fails
        """
        if not bundle_path.exists():
            raise SupportBundleError(f"Support bundle not found: {bundle_path}")
        
        if not self._is_support_bundle(bundle_path):
            raise SupportBundleError(f"Invalid support bundle format: {bundle_path}")
        
        try:
            extracted_dir = bundle_path.parent / bundle_path.stem.replace('.tar', '').replace('.tgz', '')
            
            # Extract the main archive
            with tarfile.open(bundle_path, "r:gz") as tar:
                tar.extractall(bundle_path.parent)
            
            # Extract nested archives
            self._extract_nested_archives(extracted_dir)
            
            logger.info(f"Successfully extracted support bundle to: {extracted_dir}")
            return extracted_dir
            
        except Exception as e:
            raise SupportBundleError(f"Failed to extract support bundle: {e}")
    
    def _is_support_bundle(self, file_path: Path) -> bool:
        """Check if a file is a valid support bundle."""
        return any(file_path.name.endswith(ext) for ext in self.archive_extensions)
    
    def _extract_nested_archives(self, directory: Path) -> None:
        """Extract all nested tar archives in a directory."""
        extracted_files = set()
        
        while True:
            archive_files = self._find_archive_files(directory)
            if not archive_files or all(f in extracted_files for f in archive_files):
                break
            
            for archive_file in archive_files:
                if archive_file not in extracted_files:
                    try:
                        with tarfile.open(archive_file, "r:gz") as tar:
                            tar.extractall(archive_file.parent)
                        extracted_files.add(archive_file)
                        logger.debug(f"Extracted nested archive: {archive_file}")
                    except Exception as e:
                        logger.warning(f"Failed to extract nested archive {archive_file}: {e}")
    
    def _find_archive_files(self, directory: Path) -> List[Path]:
        """Find all archive files in a directory."""
        archive_files = []
        for file_path in directory.rglob("*"):
            if file_path.is_file() and self._is_support_bundle(file_path):
                archive_files.append(file_path)
        return archive_files
    
    def find_log_files(self, directory: Path) -> List[Path]:
        """
        Find all log files in a directory.
        
        Args:
            directory: Directory to search for log files
            
        Returns:
            List of log file paths
        """
        log_files = []
        
        for file_path in directory.rglob("*"):
            if (file_path.is_file() and 
                file_path.suffix in self.supported_extensions and
                self._is_log_file(file_path)):
                log_files.append(file_path)
        
        logger.info(f"Found {len(log_files)} log files in {directory}")
        return log_files
    
    def _is_log_file(self, file_path: Path) -> bool:
        """Check if a file is a log file based on name patterns."""
        filename = file_path.name.lower()
        
        # Check for log file patterns
        log_patterns = [
            'info', 'warn', 'error', 'fatal', 'postgres',
            'tserver', 'master', 'controller', 'application'
        ]
        
        return any(pattern in filename for pattern in log_patterns)
    
    def read_log_file(self, file_path: Path) -> Iterator[str]:
        """
        Read a log file line by line, handling compression.
        
        Args:
            file_path: Path to the log file
            
        Yields:
            Lines from the log file
            
        Raises:
            FileProcessingError: If file cannot be read
        """
        try:
            if file_path.suffix == '.gz':
                with gzip.open(file_path, 'rt', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        yield line.rstrip('\n')
            else:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        yield line.rstrip('\n')
        except Exception as e:
            raise FileProcessingError(f"Failed to read log file {file_path}: {e}")
    
    def get_file_metadata(self, file_path: Path) -> Optional[LogFileMetadata]:
        """
        Extract metadata from a log file.
        
        Args:
            file_path: Path to the log file
            
        Returns:
            LogFileMetadata object or None if metadata cannot be extracted
        """
        try:
            # Read first and last few lines to get time range
            start_time = self._extract_start_time(file_path)
            end_time = self._extract_end_time(file_path)
            
            if not start_time or not end_time:
                logger.warning(f"Could not extract time range from {file_path}")
                return None
            
            # Extract other metadata
            node_name = self._extract_node_name(file_path)
            log_type = self._extract_log_type(file_path)
            sub_type = self._extract_sub_type(file_path)
            
            return LogFileMetadata(
                file_path=file_path,
                node_name=node_name,
                log_type=log_type,
                sub_type=sub_type,
                start_time=start_time,
                end_time=end_time
            )
            
        except Exception as e:
            logger.error(f"Failed to extract metadata from {file_path}: {e}")
            return None
    
    def _extract_start_time(self, file_path: Path) -> Optional[datetime]:
        """Extract the start time from a log file."""
        try:
            for line in self.read_log_file(file_path):
                timestamp = self._parse_timestamp(line)
                if timestamp:
                    return timestamp
                # Only check first 10 lines
                if line.startswith('I') or line.startswith('W') or line.startswith('E'):
                    break
        except Exception as e:
            logger.debug(f"Failed to extract start time from {file_path}: {e}")
        
        return None
    
    def _extract_end_time(self, file_path: Path) -> Optional[datetime]:
        """Extract the end time from a log file."""
        try:
            lines = list(self.read_log_file(file_path))
            for line in reversed(lines[-10:]):  # Check last 10 lines
                timestamp = self._parse_timestamp(line)
                if timestamp:
                    return timestamp
        except Exception as e:
            logger.debug(f"Failed to extract end time from {file_path}: {e}")
        
        return None
    
    def _parse_timestamp(self, line: str) -> Optional[datetime]:
        """Parse timestamp from a log line."""
        try:
            # Handle different timestamp formats
            if line.startswith(('I', 'W', 'E', 'F')):
                # Format: I1231 10:30:45.123456
                parts = line.split()
                if len(parts) >= 2:
                    time_str = parts[0][1:] + " " + parts[1][:5]
                    return datetime.strptime(time_str, "%m%d %H:%M").replace(
                        year=datetime.now().year
                    )
            else:
                # Format: 2023-12-31 10:30:45.123456
                parts = line.split()
                if len(parts) >= 2:
                    time_str = parts[0] + " " + parts[1]
                    return datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
        except (ValueError, IndexError):
            pass
        
        return None
    
    def _extract_node_name(self, file_path: Path) -> str:
        """Extract node name from file path."""
        import re
        
        # Look for node patterns in the path
        node_patterns = [
            r"/(yb-[^/]*n\d+)/",
            r"/(yb-(master|tserver)-\d+_[^/]+)/",
            r"/([^/]+-node-\d+)/"
        ]
        
        path_str = str(file_path)
        for pattern in node_patterns:
            match = re.search(pattern, path_str)
            if match:
                return match.group(1).replace("/", "")
        
        return "unknown"
    
    def _extract_log_type(self, file_path: Path) -> str:
        """Extract log type from file path."""
        filename = file_path.name.lower()
        
        if "postgres" in filename:
            return "postgres"
        elif "controller" in filename:
            return "yb-controller"
        elif "tserver" in filename:
            return "yb-tserver"
        elif "master" in filename:
            return "yb-master"
        elif "application" in filename:
            return "YBA"
        else:
            return "unknown"
    
    def _extract_sub_type(self, file_path: Path) -> str:
        """Extract sub type from file path."""
        filename = file_path.name.upper()
        
        if "INFO" in filename:
            return "INFO"
        elif "WARN" in filename:
            return "WARN"
        elif "ERROR" in filename:
            return "ERROR"
        elif "FATAL" in filename:
            return "FATAL"
        elif "postgres" in file_path.name.lower():
            return "INFO"
        elif "application" in file_path.name.lower():
            return "INFO"
        else:
            return "unknown" 