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
        patterns: List[str],
        num_threads: int = 10
    ) -> Dict[str, Any]:
        """
        Optimized: Analyze Parquet files for log patterns using DuckDB aggregation, running queries in parallel for each pattern.
        Args:
            parquet_dir: Directory containing Parquet files
            patterns: List of regex patterns to search for
        Returns:
            Dictionary with analysis results
        Raises:
            AnalysisError: If analysis fails
        """
        import concurrent.futures
        start_total = time.time()
        try:
            logger.info(f"🚀 Starting Parquet analysis (DuckDB aggregation, parallel with {num_threads} threads)...")
            bundle_name = self.get_bundle_name_from_parquet(parquet_dir)
            parquet_path = str(parquet_dir / "*.parquet")
            node_results = {}

            def run_pattern_query(pattern):
                import duckdb
                safe_pattern = pattern.replace("'", "''").replace("\\", "\\\\")
                sql = f"""
                    SELECT node_name, log_type,
                           MIN(timestamp) AS start_time,
                           MAX(timestamp) AS end_time,
                           COUNT(*) AS total_count,
                           strftime(timestamp, '%Y-%m-%dT%H:%M:00Z') AS minute,
                           COUNT(*) AS minute_count
                    FROM '{parquet_path}'
                    WHERE node_name IS NOT NULL AND REGEXP_MATCHES(message, '(?i){safe_pattern}')
                    GROUP BY node_name, log_type, minute
                """
                logger.info(f"DuckDB aggregation for pattern: {pattern}")
                con = duckdb.connect()
                rows = con.execute(sql).fetchall()
                con.close()
                return pattern, rows

            # Use ThreadPoolExecutor for parallel pattern queries
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                future_to_pattern = {executor.submit(run_pattern_query, pattern): pattern for pattern in patterns}
                for future in concurrent.futures.as_completed(future_to_pattern):
                    pattern = future_to_pattern[future]
                    try:
                        pattern, rows = future.result()
                        for row in rows:
                            node_name, log_type, start_time, end_time, total_count, minute, minute_count = row
                            if node_name not in node_results:
                                node_results[node_name] = {}
                            if log_type not in node_results[node_name]:
                                node_results[node_name][log_type] = {"logMessages": {}}
                            log_messages = node_results[node_name][log_type]["logMessages"]
                            if pattern not in log_messages:
                                log_messages[pattern] = {
                                    "StartTime": start_time,
                                    "EndTime": end_time,
                                    "count": 0,
                                    "histogram": {}
                                }
                            log_messages[pattern]["count"] += minute_count
                            log_messages[pattern]["histogram"][minute] = minute_count
                            if log_messages[pattern]["StartTime"] is None or start_time < log_messages[pattern]["StartTime"]:
                                log_messages[pattern]["StartTime"] = start_time
                            if log_messages[pattern]["EndTime"] is None or end_time > log_messages[pattern]["EndTime"]:
                                log_messages[pattern]["EndTime"] = end_time
                    except Exception as exc:
                        logger.error(f"Pattern {pattern} generated an exception: {exc}")

            total_time = time.time() - start_total
            logger.info(f"✅ DuckDB aggregation (parallel) completed in {total_time:.2f} seconds.")
            
            # Collect long operations data
            logger.info("📊 Collecting long operations data from parquet files...")
            long_ops_data = self.get_long_operations_data(parquet_dir)
            
            result = {
                "nodes": node_results,
                "universeName": bundle_name,
                "long_operations": long_ops_data
            }
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
        Save analysis results to file, converting datetime objects to ISO strings.
        Args:
            result: Analysis results dictionary
            output_path: Path to save results
        Raises:
            AnalysisError: If saving fails
        """
        def convert(obj):
            if isinstance(obj, dict):
                return {k: convert(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert(v) for v in obj]
            elif isinstance(obj, tuple):
                return tuple(convert(v) for v in obj)
            elif hasattr(obj, 'isoformat'):
                return obj.isoformat()
            else:
                return obj
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            serializable_result = convert(result)
            with open(output_path, "w") as f:
                json.dump(serializable_result, f, indent=2)
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
    
    def get_long_operations_data(self, parquet_dir: Path) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        Collect long operations data from Parquet files.
        
        This method queries parquet files for long operation messages from all files,
        handling both long_op_value and timing_real_value.
        
        Only includes messages containing:
        - "took a long time" (extracts 2 words before this phrase)
        - "Time spent" (extracts 2 words after this phrase)
        
        Aggregates by message prefix and time interval.
        
        Args:
            parquet_dir: Directory containing Parquet files
            
        Returns:
            Nested dictionary structure:
            {
                "message_prefix": {
                    "time_interval": {
                        "c": occurrence_count,
                        "avg": average_duration,
                        "max": max_duration
                    }
                }
            }
            Field names are optimized: c=count, avg=average, max=maximum
        """
        try:
            parquet_path = str(parquet_dir / "*.parquet")
            
            # Query to extract long operations data from all files
            # Handles both long_op_value and timing_real_value
            # Extracts message prefixes from various formats
            escaped_path = parquet_path.replace("'", "''")
            
            sql = f"""
                SELECT 
                    "timestamp",
                    file,
                    message_prefix,
                    op_value
                FROM (
                    SELECT 
                        "timestamp",
                        file,
                        -- Extract message prefix: 1-2 words before "took a long" (with or without "time") or 1-2 words after "Time spent"
                        -- For "took a long" patterns, extract up to 2 words before the phrase
                        -- For "Time spent" patterns, extract up to 2 words after the phrase
                        COALESCE(
                            -- Pattern 1: Extract 1-2 words before "took a long time" (prefer 2 words, fallback to 1)
                            CASE 
                                WHEN message LIKE '%took a long time%'
                                THEN TRIM(COALESCE(
                                    NULLIF(regexp_extract(message, '([^\\s]+\\s+[^\\s]+)\\s+took a long time', 1), ''),
                                    NULLIF(regexp_extract(message, '([^\\s]+)\\s+took a long time', 1), '')
                                ))
                                ELSE NULL
                            END,
                            -- Pattern 2: Extract 1-2 words before "took a long" (without "time", prefer 2 words, fallback to 1)
                            -- This handles: "StartRemoteBootstrap took a long", "Log callback took a long", "Read took a long"
                            CASE 
                                WHEN message LIKE '%took a long%' AND message NOT LIKE '%took a long time%'
                                THEN TRIM(COALESCE(
                                    -- Try to match 2 words first: "Log callback took a long" -> "Log callback"
                                    NULLIF(regexp_extract(message, '([^\\s]+\\s+[^\\s]+)\\s+took a long', 1), ''),
                                    -- Fallback to 1 word: "Read took a long" -> "Read", "StartRemoteBootstrap took a long" -> "StartRemoteBootstrap"
                                    NULLIF(regexp_extract(message, '([^\\s]+)\\s+took a long', 1), '')
                                ))
                                ELSE NULL
                            END,
                            -- Pattern 3: Extract 1-2 words after "Time spent" (prefer 2 words, fallback to 1)
                            CASE 
                                WHEN message LIKE 'Time spent%'
                                THEN TRIM(COALESCE(
                                    NULLIF(regexp_extract(message, 'Time spent\\s+([^\\s]+\\s+[^\\s]+)', 1), ''),
                                    NULLIF(regexp_extract(message, 'Time spent\\s+([^\\s]+)', 1), '')
                                ))
                                ELSE NULL
                            END
                        ) AS message_prefix,
                        -- Use COALESCE to get value from either column
                        COALESCE(long_op_value, timing_real_value) AS op_value
                    FROM '{escaped_path}'
                    WHERE (long_op_value IS NOT NULL OR timing_real_value IS NOT NULL)
                      AND (message LIKE '%took a long%' OR message LIKE 'Time spent%')
                )
                WHERE message_prefix IS NOT NULL
                  AND op_value IS NOT NULL
                  AND LENGTH(TRIM(message_prefix)) > 0
            """
            
            con = duckdb.connect()
            rows = con.execute(sql).fetchall()
            con.close()
            
            # Group by message_prefix first, then by 10-minute time buckets, calculate aggregates
            grouped_data = defaultdict(lambda: defaultdict(list))
            
            for row in rows:
                timestamp, file, message_prefix, op_value = row
                if timestamp and message_prefix and op_value is not None:
                    # Convert timestamp to datetime if needed
                    if isinstance(timestamp, str):
                        try:
                            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        except:
                            continue
                    elif isinstance(timestamp, datetime):
                        dt = timestamp
                    else:
                        continue
                    
                    # Clean up message prefix: trim whitespace and normalize common patterns
                    message_prefix = message_prefix.strip()
                    
                    # Remove IDs in parentheses (e.g., "running LogGCOp(fab8633b...)" -> "running LogGCOp")
                    # This groups operations with different IDs together
                    message_prefix = re.sub(r'\([^)]+\)', '', message_prefix)
                    message_prefix = message_prefix.strip()
                    
                    # Remove trailing colons and other punctuation
                    message_prefix = re.sub(r'[:;]+$', '', message_prefix).strip()
                    
                    # Bucket to 10-minute intervals: round down to nearest 10 minutes
                    bucket_minute = (dt.minute // 10) * 10
                    time_bucket = dt.replace(minute=bucket_minute, second=0, microsecond=0)
                    # Format to match user's expected format: 'YYYY-MM-DD HH:MM:00'
                    time_key = time_bucket.strftime('%Y-%m-%d %H:%M:00')
                    
                    grouped_data[message_prefix][time_key].append(op_value)
            
            # Build nested result structure with optimized field names
            result = {}
            for message_prefix, time_intervals in sorted(grouped_data.items()):
                result[message_prefix] = {}
                for time_interval, values in sorted(time_intervals.items()):
                    if values:  # Only add if we have values
                        result[message_prefix][time_interval] = {
                            "c": len(values),  # count
                            "avg": sum(values) / len(values),  # average
                            "max": max(values)  # maximum
                        }
            
            total_records = sum(len(intervals) for intervals in result.values())
            logger.info(f"Collected {total_records} long operations records across {len(result)} message prefixes")
            if result:
                sample_prefixes = list(result.keys())[:10]
                logger.debug(f"Sample message prefixes: {sample_prefixes}")
            return result
            
        except Exception as e:
            logger.warning(f"Failed to collect long operations data: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return []
    
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