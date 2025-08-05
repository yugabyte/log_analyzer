"""
Main analysis service for log processing.

This module provides the main service for orchestrating log analysis,
including parallel processing, result aggregation, and report generation.
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from multiprocessing import Pool
import logging
import threading
import time

from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TaskProgressColumn

from colorama import Fore, Style

from models.log_metadata import (
    AnalysisReport, 
    SupportBundleInfo, 
    AnalysisConfig,
    NodeAnalysisResult,
    LogMessageStats
)
from services.file_processor import FileProcessor
from services.pattern_matcher import PatternMatcher
from utils.exceptions import AnalysisError, ValidationError
from config.settings import settings


logger = logging.getLogger(__name__)


class AnalysisService:
    """Main service for log analysis orchestration."""
    
    def __init__(self):
        self.file_processor = FileProcessor()
        self.pattern_matcher = PatternMatcher()
    
    def analyze_support_bundle(
        self,
        bundle_path: Path,
        analysis_config: AnalysisConfig,
        skip_extraction: bool = False
    ) -> AnalysisReport:
        """
        Analyze a support bundle and generate a report.
        
        Args:
            bundle_path: Path to the support bundle
            analysis_config: Configuration for analysis
            skip_extraction: Whether to skip bundle extraction
            
        Returns:
            AnalysisReport with results
            
        Raises:
            AnalysisError: If analysis fails
        """
        try:
            # Validate configuration
            analysis_config.validate()
            
            # Extract support bundle if needed
            if not skip_extraction:
                extracted_dir = self.file_processor.extract_support_bundle(bundle_path)
            else:
                extracted_dir = bundle_path.parent / bundle_path.stem.replace('.tar', '').replace('.tgz', '')
            
            # Build support bundle info
            support_bundle_info = self._build_support_bundle_info(extracted_dir, bundle_path.name)
            
            # Filter nodes and log types if specified
            self._apply_filters(support_bundle_info, analysis_config)
            
            # Analyze logs
            analysis_results = self._analyze_logs(support_bundle_info, analysis_config)
            
            # Generate report
            report = self._generate_report(support_bundle_info, analysis_results, analysis_config)
            
            return report
            
        except Exception as e:
            raise AnalysisError(f"Analysis failed: {e}", details={"bundle_path": str(bundle_path)})
    
    def _build_support_bundle_info(
        self, 
        extracted_dir: Path, 
        bundle_name: str
    ) -> SupportBundleInfo:
        """Build support bundle information from extracted directory."""
        # Find log files
        log_files = self.file_processor.find_log_files(extracted_dir)
        
        # Extract metadata for each log file
        metadata_by_node = {}
        metadata_for_json = {}

        for log_file in log_files:
            metadata = self.file_processor.get_file_metadata(log_file)
            if not metadata:
                continue
            
            # Organize by node -> log_type -> sub_type -> file_path -> metadata
            node_name = metadata.node_name
            log_type = metadata.log_type
            sub_type = metadata.sub_type
            
            if node_name not in metadata_by_node:
                metadata_by_node[node_name] = {}
            if log_type not in metadata_by_node[node_name]:
                metadata_by_node[node_name][log_type] = {}
            if sub_type not in metadata_by_node[node_name][log_type]:
                metadata_by_node[node_name][log_type][sub_type] = {}
            metadata_by_node[node_name][log_type][sub_type][str(log_file)] = metadata

            # For JSON: only logStartsAt and logEndsAt as strings
            if node_name not in metadata_for_json:
                metadata_for_json[node_name] = {}
            if log_type not in metadata_for_json[node_name]:
                metadata_for_json[node_name][log_type] = {}
            if sub_type not in metadata_for_json[node_name][log_type]:
                metadata_for_json[node_name][log_type][sub_type] = {}
            metadata_for_json[node_name][log_type][sub_type][str(log_file)] = {
                "logStartsAt": metadata.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "logEndsAt": metadata.end_time.strftime("%Y-%m-%d %H:%M:%S")
            }
        
        # Dump metadata to JSON for debugging (original format)
        metadata_json_path = extracted_dir / "log_file_metadata.json"
        with open(metadata_json_path, 'w') as f:
            json.dump(metadata_for_json, f, indent=2)
        
        logger.info(f"Metadata written to {metadata_json_path}")
        
        return SupportBundleInfo(
            name=bundle_name.replace('.tar.gz', '').replace('.tgz', ''),
            directory=extracted_dir.parent,
            extracted_path=extracted_dir,
            log_files_metadata=metadata_by_node
        )
    
    def _apply_filters(
        self, 
        support_bundle_info: SupportBundleInfo, 
        analysis_config: AnalysisConfig
    ) -> None:
        """Apply node and log type filters to support bundle info."""
        # Filter nodes
        if analysis_config.node_filter:
            filtered_metadata = {}
            for node_name in analysis_config.node_filter:
                if node_name in support_bundle_info.log_files_metadata:
                    filtered_metadata[node_name] = support_bundle_info.log_files_metadata[node_name]
            support_bundle_info.log_files_metadata = filtered_metadata
        
        # Filter log types
        if analysis_config.log_type_filter:
            for node_name in support_bundle_info.log_files_metadata:
                node_data = support_bundle_info.log_files_metadata[node_name]
                filtered_node_data = {}
                
                for log_type in analysis_config.log_type_filter:
                    if log_type in node_data:
                        filtered_node_data[log_type] = node_data[log_type]
                
                support_bundle_info.log_files_metadata[node_name] = filtered_node_data
    
    def _analyze_logs(
        self, 
        support_bundle_info: SupportBundleInfo, 
        analysis_config: AnalysisConfig
    ) -> Dict[str, Dict[str, NodeAnalysisResult]]:
        """Analyze logs using parallel processing with a rich progress bar."""
        # Prepare tasks for parallel processing
        tasks = []
        task_id = 0
        for node_name, node_data in support_bundle_info.log_files_metadata.items():
            for log_type, log_type_data in node_data.items():
                for sub_type, sub_type_data in log_type_data.items():
                    # Filter files by time range
                    filtered_files = self._filter_files_by_time(
                        sub_type_data, 
                        analysis_config.start_time, 
                        analysis_config.end_time
                    )
                    if filtered_files:
                        tasks.append((
                            task_id,
                            node_name,
                            log_type,
                            sub_type,
                            filtered_files,
                            analysis_config
                        ))
                        task_id += 1
        # Process tasks in parallel with rich progress bar
        if tasks:
            results = []
            with Pool(processes=analysis_config.parallel_threads) as pool:
                total = len(tasks)
                width = len(str(total))
                columns = [
                    TextColumn("[cyan]Analyzing support bundle...."),
                    BarColumn(),
                    TaskProgressColumn(),
                    TextColumn(f"[{{task.completed:0{width}d}}/{{task.total:0{width}d}}]"),
                    TimeElapsedColumn()
                ]
                with Progress(*columns) as progress:
                    task_id_progress = progress.add_task("parse", total=total)
                    for result in pool.imap_unordered(self._analyze_node_logs_worker, tasks):
                        results.append(result)
                        progress.update(task_id_progress, advance=1)
            # Aggregate results
            aggregated_results = {}
            for result in results:
                if result:
                    node_name = result.node_name
                    log_type = result.log_type
                    if node_name not in aggregated_results:
                        aggregated_results[node_name] = {}
                    if log_type not in aggregated_results[node_name]:
                        aggregated_results[node_name][log_type] = result
                    else:
                        # Merge log messages
                        for pattern_name, stats in result.log_messages.items():
                            if pattern_name in aggregated_results[node_name][log_type].log_messages:
                                existing_stats = aggregated_results[node_name][log_type].log_messages[pattern_name]
                                existing_stats.count += stats.count
                                existing_stats.start_time = min(existing_stats.start_time, stats.start_time)
                                existing_stats.end_time = max(existing_stats.end_time, stats.end_time)
                                for time_key, count in stats.histogram.items():
                                    existing_stats.histogram[time_key] = (
                                        existing_stats.histogram.get(time_key, 0) + count
                                    )
                            else:
                                aggregated_results[node_name][log_type].log_messages[pattern_name] = stats
            return aggregated_results
        return {}
    
    def _filter_files_by_time(
        self, 
        files_metadata: Dict[str, Any], 
        start_time: datetime, 
        end_time: datetime
    ) -> List[str]:
        """Filter files by time range."""
        filtered_files = []
        
        for file_path, metadata in files_metadata.items():
            file_start = metadata.start_time
            file_end = metadata.end_time
            
            # Check if file overlaps with time range
            if (file_start <= end_time and file_end >= start_time):
                filtered_files.append(file_path)
        
        return filtered_files
    
    def _analyze_node_logs_worker(self, task: Tuple) -> Optional[NodeAnalysisResult]:
        """Worker function for parallel log analysis."""
        try:
            task_id, node_name, log_type, sub_type, file_paths, analysis_config = task
            
            # Get patterns for log type
            if analysis_config.histogram_mode:
                patterns = self.pattern_matcher.get_custom_patterns(analysis_config.histogram_mode)
            else:
                patterns = self.pattern_matcher.get_patterns_for_log_type(log_type)
            
            if not patterns:
                logger.warning(f"No patterns found for log type: {log_type}")
                return None
            
            # Analyze each file
            all_message_stats = {}
            
            for file_path in file_paths:
                file_stats = self.pattern_matcher.analyze_log_file(
                    file_path,
                    patterns,
                    analysis_config.start_time,
                    analysis_config.end_time
                )
                
                # Merge statistics
                for pattern_name, stats in file_stats.items():
                    if pattern_name in all_message_stats:
                        existing_stats = all_message_stats[pattern_name]
                        existing_stats.count += stats.count
                        existing_stats.start_time = min(existing_stats.start_time, stats.start_time)
                        existing_stats.end_time = max(existing_stats.end_time, stats.end_time)
                        
                        # Merge histogram
                        for time_key, count in stats.histogram.items():
                            existing_stats.histogram[time_key] = (
                                existing_stats.histogram.get(time_key, 0) + count
                            )
                    else:
                        all_message_stats[pattern_name] = stats
            
            return NodeAnalysisResult(
                node_name=node_name,
                log_type=log_type,
                log_messages=all_message_stats
            )
            
        except Exception as e:
            logger.error(f"Error in analysis worker: {e}")
            return None
    
    def _generate_report(
        self,
        support_bundle_info: SupportBundleInfo,
        analysis_results: Dict[str, Dict[str, NodeAnalysisResult]],
        analysis_config: AnalysisConfig
    ) -> AnalysisReport:
        """Generate the final analysis report."""
        # Collect warnings
        warnings = self._collect_warnings(support_bundle_info, analysis_config)
        
        # Build report
        report = AnalysisReport(
            support_bundle_name=support_bundle_info.name,
            nodes=analysis_results,
            warnings=warnings,
            analysis_config={
                "start_time": analysis_config.start_time.isoformat(),
                "end_time": analysis_config.end_time.isoformat(),
                "parallel_threads": analysis_config.parallel_threads,
                "node_filter": analysis_config.node_filter,
                "log_type_filter": analysis_config.log_type_filter,
                "histogram_mode": analysis_config.histogram_mode
            }
        )
        
        return report
    
    def _collect_warnings(
        self, 
        support_bundle_info: SupportBundleInfo, 
        analysis_config: AnalysisConfig
    ) -> List[Dict[str, Any]]:
        """Collect warnings for the report."""
        warnings = []
        
        # Add custom options warning
        custom_opts = []
        if analysis_config.node_filter:
            custom_opts.append(f"node_filter: {analysis_config.node_filter}")
        if analysis_config.log_type_filter:
            custom_opts.append(f"log_type_filter: {analysis_config.log_type_filter}")
        if analysis_config.histogram_mode:
            custom_opts.append(f"histogram_mode: {analysis_config.histogram_mode}")
        
        if custom_opts:
            warnings.append({
                "message": "This report was generated with custom options. Results may not include all logs.",
                "additional_details": f"Custom options used: {'; '.join(custom_opts)}",
                "level": "info",
                "type": "custom_options"
            })
        
        return warnings
    
    def save_report(self, report: AnalysisReport, output_path: Path) -> None:
        """Save analysis report to file."""
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w') as f:
                json.dump(report.to_dict(), f, indent=2)
            
            logger.info(f"Report saved to: {output_path}")
            
        except Exception as e:
            raise AnalysisError(f"Failed to save report: {e}")
    
    def load_report(self, report_path: Path) -> AnalysisReport:
        """Load analysis report from file."""
        try:
            with open(report_path, 'r') as f:
                report_data = json.load(f)
            
            # Convert back to AnalysisReport object
            # This is a simplified conversion - in a real implementation,
            # you'd want more robust deserialization
            return AnalysisReport(
                support_bundle_name=report_data.get("support_bundle_name", ""),
                nodes=report_data.get("nodes", {}),
                warnings=report_data.get("warnings", []),
                analysis_config=report_data.get("analysis_config", {})
            )
            
        except Exception as e:
            raise AnalysisError(f"Failed to load report: {e}")