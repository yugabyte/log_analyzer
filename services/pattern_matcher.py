"""
Pattern matching service for log analysis.

This module provides services for matching log patterns and extracting
statistics from log files with proper error handling and performance optimization.
"""

import re
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import logging
from collections import defaultdict

from models.log_metadata import LogMessageStats
from config.settings import settings


logger = logging.getLogger(__name__)


class PatternMatcher:
    """Service for matching log patterns and extracting statistics."""
    
    def __init__(self):
        self.patterns_cache: Dict[str, re.Pattern] = {}
        self._load_patterns()
    
    def _load_patterns(self) -> None:
        """Load patterns from configuration."""
        try:
            import yaml
            from pathlib import Path
            
            config_path = settings.log_conf_path
            if not config_path.exists():
                logger.warning(f"Pattern configuration file not found: {config_path}")
                return
            
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            # Load universe patterns
            universe_config = config.get("universe", {}).get("log_messages", [])
            for msg_dict in universe_config:
                name = msg_dict["name"]
                pattern = msg_dict["pattern"]
                self.patterns_cache[name] = re.compile(pattern, re.IGNORECASE)
            
            # Load PostgreSQL patterns
            pg_config = config.get("pg", {}).get("log_messages", [])
            for msg_dict in pg_config:
                name = msg_dict["name"]
                pattern = msg_dict["pattern"]
                self.patterns_cache[name] = re.compile(pattern, re.IGNORECASE)
            
            logger.info(f"Loaded {len(self.patterns_cache)} patterns")
            
        except Exception as e:
            logger.error(f"Failed to load patterns: {e}")
    
    def get_patterns_for_log_type(self, log_type: str) -> Dict[str, re.Pattern]:
        """
        Get patterns for a specific log type.
        
        Args:
            log_type: Type of log (postgres, yb-tserver, etc.)
            
        Returns:
            Dictionary of pattern names to compiled patterns
        """
        if log_type == "postgres":
            return {name: pattern for name, pattern in self.patterns_cache.items() 
                   if name in self._get_pg_pattern_names()}
        else:
            return {name: pattern for name, pattern in self.patterns_cache.items() 
                   if name in self._get_universe_pattern_names()}
    
    def _get_pg_pattern_names(self) -> List[str]:
        """Get PostgreSQL pattern names."""
        return [
            "connection_error",
            "query_timeout",
            "deadlock_detected",
            "insufficient_memory",
            "disk_full",
            "authentication_failed"
        ]
    
    def _get_universe_pattern_names(self) -> List[str]:
        """Get universe pattern names."""
        return [
            "tablet_not_found",
            "leader_not_ready",
            "network_timeout",
            "consensus_error",
            "compaction_error",
            "memory_pressure"
        ]
    
    def match_line(self, line: str, patterns: Dict[str, re.Pattern]) -> Optional[Tuple[str, re.Match]]:
        """
        Match a log line against patterns.
        
        Args:
            line: Log line to match
            patterns: Dictionary of pattern names to compiled patterns
            
        Returns:
            Tuple of (pattern_name, match_object) or None if no match
        """
        for pattern_name, pattern in patterns.items():
            match = pattern.search(line)
            if match:
                return pattern_name, match
        
        return None
    
    def analyze_log_file(
        self,
        file_path: str,
        patterns: Dict[str, re.Pattern],
        start_time: datetime,
        end_time: datetime,
        progress_callback: Optional[callable] = None
    ) -> Dict[str, LogMessageStats]:
        """
        Analyze a log file for pattern matches.
        
        Args:
            file_path: Path to the log file
            patterns: Dictionary of pattern names to compiled patterns
            start_time: Start time for analysis
            end_time: End time for analysis
            progress_callback: Optional callback for progress updates
            
        Returns:
            Dictionary of pattern names to LogMessageStats
        """
        from services.file_processor import FileProcessor
        
        file_processor = FileProcessor()
        message_stats: Dict[str, LogMessageStats] = {}
        
        try:
            for line_num, line in enumerate(file_processor.read_log_file(file_path)):
                # Parse timestamp from line
                timestamp = self._parse_timestamp(line)
                if not timestamp:
                    continue
                
                # Check if line is within time range
                if not (start_time <= timestamp <= end_time):
                    continue
                
                # Match patterns
                match_result = self.match_line(line, patterns)
                if match_result:
                    pattern_name, match = match_result
                    
                    # Update statistics
                    if pattern_name not in message_stats:
                        message_stats[pattern_name] = LogMessageStats(
                            pattern_name=pattern_name,
                            start_time=timestamp,
                            end_time=timestamp,
                            count=1
                        )
                    else:
                        stats = message_stats[pattern_name]
                        stats.count += 1
                        stats.start_time = min(stats.start_time, timestamp)
                        stats.end_time = max(stats.end_time, timestamp)
                    
                    # Update histogram
                    minute_key = timestamp.replace(second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:00Z')
                    message_stats[pattern_name].histogram[minute_key] = (
                        message_stats[pattern_name].histogram.get(minute_key, 0) + 1
                    )
                
                # Progress callback
                if progress_callback and line_num % 1000 == 0:
                    progress_callback(line_num)
                    
        except Exception as e:
            logger.error(f"Error analyzing log file {file_path}: {e}")
        
        return message_stats
    
    def _parse_timestamp(self, line: str) -> Optional[datetime]:
        """
        Parse timestamp from a log line.
        
        Args:
            line: Log line to parse
            
        Returns:
            Parsed datetime or None if parsing fails
        """
        try:
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
    
    def get_custom_patterns(self, pattern_string: str) -> Dict[str, re.Pattern]:
        """
        Create custom patterns from a comma-separated string.
        
        Args:
            pattern_string: Comma-separated list of regex patterns
            
        Returns:
            Dictionary of pattern names to compiled patterns
        """
        patterns = {}
        pattern_list = [p.strip() for p in pattern_string.split(',') if p.strip()]
        
        for i, pattern in enumerate(pattern_list):
            try:
                pattern_name = f"custom_pattern_{i}"
                patterns[pattern_name] = re.compile(pattern, re.IGNORECASE)
            except re.error as e:
                logger.warning(f"Invalid regex pattern '{pattern}': {e}")
        
        return patterns 