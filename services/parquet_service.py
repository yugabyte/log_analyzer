"""
Parquet file analysis service.

This module provides services for analyzing log data stored in Parquet format
with efficient querying and pattern matching capabilities.
"""

import os
import json
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
from collections import defaultdict
import logging

import duckdb
import glob
import psycopg2

from utils.exceptions import AnalysisError
from config.settings import settings
from services.database_service import DatabaseService


# Use the same logger as the main application
logger = logging.getLogger("log_analyzer")


class ParquetAnalysisService:
    """Service for analyzing Parquet files."""
    
    def __init__(self):
        self.process_types = settings.analysis_config.supported_process_types
    
    def get_default_patterns(self) -> List[str]:
        """Get default patterns for Parquet analysis."""
        try:
            import yaml
            
            config_path = settings.log_conf_path
            if not config_path.exists():
                logger.warning(f"Pattern configuration file not found: {config_path}")
                return []
            
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            patterns = []
            
            # Add universe patterns
            universe_config = config.get("universe", {}).get("log_messages", [])
            for msg_dict in universe_config:
                patterns.append(msg_dict["pattern"])
            
            # Add PostgreSQL patterns
            pg_config = config.get("pg", {}).get("log_messages", [])
            for msg_dict in pg_config:
                patterns.append(msg_dict["pattern"])
            
            return patterns
            
        except Exception as e:
            logger.error(f"Failed to load default patterns: {e}")
            return []
    
    def get_bundle_name_from_parquet(self, parquet_dir: Path) -> str:
        """
        Extract the support_bundle name from Parquet files in the directory.
        Falls back to directory name if not found.

        Args:
            parquet_dir: Directory containing Parquet files

        Returns:
            Bundle name as string
        """
        parquet_path = str(parquet_dir / "*.parquet")
        try:
            con = duckdb.connect()
            bundle_sql = f"SELECT support_bundle FROM '{parquet_path}' LIMIT 1"
            bundle_row = con.execute(bundle_sql).fetchone()
            con.close()
            return bundle_row[0] if bundle_row and bundle_row[0] else parquet_dir.name
        except Exception as e:
            logger.warning(f"Could not extract support_bundle from Parquet: {e}")
            return parquet_dir.name

    def analyze_parquet_files(
        self, 
        parquet_dir: Path, 
        patterns: List[str]
    ) -> Dict[str, Any]:
        """
        Analyze Parquet files for log patterns.
        
        Args:
            parquet_dir: Directory containing Parquet files
            patterns: List of regex patterns to search for
            
        Returns:
            Dictionary with analysis results
            
        Raises:
            AnalysisError: If analysis fails
        """
        start_total = time.time()
        
        try:
            logger.info("🚀 Starting Parquet analysis...")
            # Extract bundle name
            bundle_name = self.get_bundle_name_from_parquet(parquet_dir)
            
            # Build DuckDB query
            parquet_path = str(parquet_dir / "*.parquet")
            # pattern_filters = self._build_pattern_filters(patterns)
            # Enhanced filtering to remove blank messages and whitespace-only messages
            pattern_filters = []
            for pattern in patterns:
                safe_pattern = pattern.replace("'", "''").replace("\\", "\\\\")
                pattern_filters.append(f"REGEXP_MATCHES(message, '(?i){safe_pattern}')")
            where_clause = "node_name IS NOT NULL AND (" + " OR ".join(pattern_filters) + ")"
            
            # Main analysis query
            sql = f"""
                SELECT node_name, log_type, timestamp, message
                FROM '{parquet_path}'
                WHERE {where_clause}
            """
            
            logger.info("📊 Executing DuckDB query with pattern filtering...")
            start_duckdb = time.time()
            
            # Execute query
            con = duckdb.connect()
            rows = con.execute(sql).fetchall()
            con.close()
            
            duckdb_time = time.time() - start_duckdb
            logger.info(f"✅ DuckDB query completed in {duckdb_time:.2f} seconds. Rows fetched: {len(rows)}")
            
            # Process results with detailed timing
            start_processing = time.time()
            node_results = self._process_query_results(rows, patterns)
            processing_time = time.time() - start_processing
            logger.info(f"✅ Pattern matching completed in {processing_time:.2f} seconds.")
            
            # Build final result with actual bundle name
            result = {
                "nodes": node_results,
                "universeName": bundle_name  # Use directory name as bundle name
            }
            
            total_time = time.time() - start_total
            logger.info(f"📈 Performance breakdown:")
            logger.info(f"   - DuckDB query: {duckdb_time:.2f}s ({(duckdb_time/total_time)*100:.1f}%)")
            logger.info(f"   - Pattern processing: {processing_time:.2f}s ({(processing_time/total_time)*100:.1f}%)")
            
            return result
            
        except Exception as e:
            raise AnalysisError(f"Parquet analysis failed: {e}")
    
    def _build_pattern_filters(self, patterns: List[str]) -> List[str]:
        """Build SQL pattern filters for DuckDB query."""
        pattern_filters = []
        
        for pattern in patterns:
            # Escape single quotes and backslashes for SQL
            safe_pattern = pattern.replace("'", "''").replace("\\", "\\\\")
            pattern_filters.append(f"REGEXP_MATCHES(message, '(?i){safe_pattern}')")
        
        return pattern_filters
    
    def _process_query_results(
        self, 
        rows: List[tuple], 
        patterns: List[str]
    ) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Process query results and build node-based statistics with a progress bar."""
        import time
        from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn

        node_proc_pat = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

        # Filter out empty patterns to prevent blank message matches
        valid_patterns = [pattern for pattern in patterns if pattern and pattern.strip()]

        # Use actual pattern names instead of generic pattern_X names
        pattern_name_map = {pattern: pattern for pattern in valid_patterns}

        logger.info("🔄 Processing rows for pattern matching...")

        # Track pattern matching timing
        start_pattern_matching = time.time()
        pattern_matches = 0
        total_rows = len(rows)
        blank_messages_skipped = 0

        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            "{task.completed}/{task.total}",
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            transient=True,
        ) as progress:
            task = progress.add_task("Processing rows", total=total_rows)
            for idx, (node_name, log_type, timestamp, message) in enumerate(rows):
                if idx % 1000 == 0 and idx > 0:
                    progress.update(task, advance=1000)
                # Enhanced filtering for blank messages
                if not message or message.strip() == "" or message.isspace():
                    blank_messages_skipped += 1
                    continue
                # Match patterns
                for pattern in valid_patterns:
                    if re.search(pattern, message):
                        pattern_name = pattern_name_map.get(pattern, pattern)
                        node_proc_pat[node_name][log_type][pattern_name].append(timestamp)
                        pattern_matches += 1
                        break  # Only match first pattern
            # Advance any remaining
            progress.update(task, completed=total_rows)

        pattern_matching_time = time.time() - start_pattern_matching
        # Log blank message count for debugging
        if blank_messages_skipped > 0:
            logger.info(f"⚠️  Skipped {blank_messages_skipped} blank/empty messages during processing")

        # Build final results
        start_building_results = time.time()
        result = {}

        for node_name in node_proc_pat:
            result[node_name] = {}
            for proc in self.process_types:
                log_messages = {}
                proc_data = node_proc_pat[node_name].get(proc, {})
                for pattern_name, timestamps in proc_data.items():
                    if timestamps:
                        times = sorted(timestamps)
                        start_time = times[0]
                        end_time = times[-1]
                        total_count = len(times)
                        histogram = defaultdict(int)
                        for t in times:
                            if isinstance(t, datetime):
                                minute = t.replace(second=0, microsecond=0)
                                minute_str = minute.strftime('%Y-%m-%dT%H:%M:00Z')
                            else:
                                dt = datetime.strptime(str(t)[:19], "%Y-%m-%d %H:%M:%S")
                                minute = dt.replace(second=0, microsecond=0)
                                minute_str = minute.strftime('%Y-%m-%dT%H:%M:00Z')
                            histogram[minute_str] += 1
                        def iso(dt):
                            if isinstance(dt, datetime):
                                return dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                            else:
                                return str(dt)[:19].replace(' ', 'T') + 'Z'
                        log_messages[pattern_name] = {
                            "StartTime": iso(start_time),
                            "EndTime": iso(end_time),
                            "count": total_count,
                            "histogram": dict(histogram)
                        }
                result[node_name][proc] = {"logMessages": log_messages}

        building_results_time = time.time() - start_building_results
        logger.info(f"📊 Results building completed in {building_results_time:.2f} seconds.")

        return result
    
    def save_results(self, result: Dict[str, Any], output_path: Path) -> None:
        """
        Save analysis results to file.
        
        Args:
            result: Analysis results dictionary
            output_path: Path to save results
            
        Raises:
            AnalysisError: If saving fails
        """
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, "w") as f:
                json.dump(result, f, indent=2)
                        
        except Exception as e:
            raise AnalysisError(f"Failed to save results: {e}")
    
    def load_results(self, output_path: Path) -> Dict[str, Any]:
        """
        Load analysis results from file.
        
        Args:
            output_path: Path to load results from
            
        Returns:
            Analysis results dictionary
            
        Raises:
            AnalysisError: If loading fails
        """
        try:
            with open(output_path, "r") as f:
                return json.load(f)
                
        except Exception as e:
            raise AnalysisError(f"Failed to load results: {e}")
    
    def get_parquet_info(self, parquet_dir: Path) -> Dict[str, Any]:
        """
        Get information about Parquet files in directory.
        
        Args:
            parquet_dir: Directory containing Parquet files
            
        Returns:
            Dictionary with Parquet file information
        """
        try:
            parquet_files = list(parquet_dir.glob("*.parquet"))
            
            if not parquet_files:
                return {"error": "No Parquet files found"}
            
            # Get basic info
            total_size = sum(f.stat().st_size for f in parquet_files)
            file_count = len(parquet_files)
            
            # Try to get schema info from first file
            schema_info = {}
            try:
                con = duckdb.connect()
                schema_sql = f"DESCRIBE SELECT * FROM '{parquet_files[0]}' LIMIT 0"
                schema_result = con.execute(schema_sql).fetchall()
                con.close()
                
                schema_info = {
                    "columns": [row[0] for row in schema_result],
                    "types": [row[1] for row in schema_result]
                }
            except Exception as e:
                logger.warning(f"Could not get schema info: {e}")
            
            return {
                "file_count": file_count,
                "total_size_bytes": total_size,
                "total_size_mb": total_size / (1024 * 1024),
                "schema": schema_info,
                "files": [str(f.name) for f in parquet_files[:10]]  # First 10 files
            }
            
        except Exception as e:
            return {"error": f"Failed to get Parquet info: {e}"}