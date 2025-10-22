#!/usr/bin/env python3
"""
Log Analyzer for YugabyteDB Support Bundles

A refactored, maintainable, and efficient log analysis tool that processes
YugabyteDB support bundles to identify patterns and generate reports.

This version follows best practices including:
- Proper separation of concerns
- Type hints throughout
- Comprehensive error handling
- Configuration management
- Structured logging
- Clean architecture
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List
import logging

from colorama import Fore, Style, init

from config.settings import settings
from utils.logging_config import setup_logging, get_logger
from utils.exceptions import (
    LogAnalyzerError, 
    ConfigurationError, 
    ValidationError,
    AnalysisError
)
from models.log_metadata import AnalysisConfig
from services.analysis_service import AnalysisService
from services.database_service import DatabaseService
from services.parquet_service import ParquetAnalysisService


# Initialize colorama for cross-platform colored output
init()


class ColoredHelpFormatter(argparse.RawTextHelpFormatter):
    """Custom argument parser formatter with colored output."""
    
    def _get_help_string(self, action):
        return Fore.GREEN + super()._get_help_string(action) + Style.RESET_ALL

    def _format_usage(self, usage, actions, groups, prefix):
        return Fore.YELLOW + super()._format_usage(usage, actions, groups, prefix) + Style.RESET_ALL
    
    def _format_action_invocation(self, action):
        if not action.option_strings:
            metavar, = self._metavar_formatter(action, action.dest)(1)
            return Fore.CYAN + metavar + Style.RESET_ALL
        else:
            parts = []
            if action.nargs == 0:
                parts.extend(action.option_strings)
            else:
                default = action.dest.upper()
                args_string = self._format_args(action, default)
                parts.extend(action.option_strings)
                parts[-1] += ' ' + args_string
            return Fore.CYAN + ', '.join(parts) + Style.RESET_ALL
    
    def _format_action(self, action):
        parts = super()._format_action(action)
        return Fore.CYAN + parts + Style.RESET_ALL
    
    def _format_text(self, text):
        return Fore.MAGENTA + super()._format_text(text) + Style.RESET_ALL
    
    def _format_args(self, action, default_metavar):
        return Fore.LIGHTCYAN_EX + super()._format_args(action, default_metavar) + Style.RESET_ALL


class LogAnalyzerApp:
    """Main application class for log analysis."""
    
    def __init__(self):
        self.logger = get_logger("log_analyzer")
        self.analysis_service = AnalysisService()
        self.database_service = DatabaseService()
        self.parquet_service = ParquetAnalysisService()
    
    def setup_argument_parser(self) -> argparse.ArgumentParser:
        """Set up command line argument parser."""
        parser = argparse.ArgumentParser(
            description="Log Analyzer for YugabyteDB logs",
            formatter_class=ColoredHelpFormatter
        )
        # Input options
        input_group = parser.add_mutually_exclusive_group(required=True)
        input_group.add_argument(
            "-s", "--support_bundle",
            help="Support bundle file name (.tar.gz or .tgz)"
        )
        input_group.add_argument(
            "--parquet_files",
            metavar="DIR",
            help="Directory containing Parquet files to analyze"
        )
        # --tablet_report is only required for direct tablet report analysis, not with -s
        parser.add_argument(
            "--tablet_report",
            metavar="DIR",
            help="Directory containing extracted TabletReport files to analyze (optional, auto-detected with -s)"
        )
        # ...existing code...
        parser.add_argument(
            "--types",
            metavar="LIST",
            help="List of log types to analyze (e.g., 'ms,ybc'). Default: 'pg,ts,ms'"
        )
        parser.add_argument(
            "-n", "--nodes",
            metavar="LIST",
            help="List of nodes to analyze (e.g., 'n1,n2')"
        )
        parser.add_argument(
            "-o", "--output",
            metavar="FILE",
            dest="output_file",
            help="Output file name for the report"
        )
        parser.add_argument(
            "-p", "--parallel",
            metavar="N",
            dest='num_threads',
            default=settings.analysis_config.default_parallel_threads,
            type=int,
            help=f"Run in parallel mode with N threads (default: {settings.analysis_config.default_parallel_threads})"
        )
        parser.add_argument(
            "--skip_tar",
            action="store_true",
            help="Skip tar file extraction (assumes already extracted)"
        )
        
        # Time range options
        parser.add_argument(
            "-t", "--from_time",
            metavar="MMDD HH:MM",
            dest="start_time",
            help="Specify start time in quotes (e.g., '1231 10:30')"
        )
        parser.add_argument(
            "-T", "--to_time",
            metavar="MMDD HH:MM",
            dest="end_time",
            help="Specify end time in quotes (e.g., '1231 23:59')"
        )
        
        # Analysis mode options
        parser.add_argument(
            "--histogram-mode",
            dest="histogram_mode",
            metavar="LIST",
            help="List of errors to generate histogram (e.g., 'error1,error2,error3')"
        )
        
        # Force option
        parser.add_argument(
            "--force",
            action="store_true",
            help="Force analysis even if report already exists"
        )
        
        return parser
    
    def validate_arguments(self, args: argparse.Namespace) -> None:
        """Validate command line arguments."""
        # Validate time format
        if args.start_time:
            try:
                datetime.strptime(args.start_time, "%m%d %H:%M")
            except ValueError:
                raise ValidationError("Incorrect start time format, should be MMDD HH:MM")
        
        if args.end_time:
            try:
                datetime.strptime(args.end_time, "%m%d %H:%M")
            except ValueError:
                raise ValidationError("Incorrect end time format, should be MMDD HH:MM")
        
        # Validate parallel threads
        if args.num_threads < 1 or args.num_threads > 20:
            raise ValidationError("Parallel threads must be between 1 and 20")
        
        # Validate parquet options
        if args.parquet_files:
            unsupported_opts = []
            if args.types:
                unsupported_opts.append("--types")
            if args.nodes:
                unsupported_opts.append("--nodes")
            if args.num_threads != settings.analysis_config.default_parallel_threads:
                unsupported_opts.append("--parallel")
            if args.skip_tar:
                unsupported_opts.append("--skip_tar")
            if args.start_time:
                unsupported_opts.append("--from_time")
            if args.end_time:
                unsupported_opts.append("--to_time")
            
            if unsupported_opts:
                raise ValidationError(
                    f"The following options are not supported with --parquet_files: {', '.join(unsupported_opts)}"
                )
    
    def parse_time_range(self, args: argparse.Namespace) -> tuple[datetime, datetime]:
        """Parse and validate time range from arguments."""
        # Calculate default time range
        default_start_time = datetime.now() - timedelta(days=settings.analysis_config.default_time_range_days)
        default_start_time = default_start_time.strftime("%m%d %H:%M")

        # Parse start time
        if args.start_time:
            start_time = datetime.strptime(args.start_time, "%m%d %H:%M")
        else:
            start_time = datetime.strptime(default_start_time, "%m%d %H:%M")
        
        # Parse end time
        if args.end_time:
            end_time = datetime.strptime(args.end_time, "%m%d %H:%M")
        else:
            end_time = datetime.now()
        
        return start_time, end_time
    
    def create_analysis_config(self, args: argparse.Namespace) -> AnalysisConfig:
        """Create analysis configuration from arguments."""
        start_time, end_time = self.parse_time_range(args)
        
        # Parse filters
        node_filter = None
        if args.nodes:
            node_filter = [n.strip().lower() for n in args.nodes.split(',') if n.strip()]
        
        log_type_filter = None
        if args.types:
            requested_types = [t.strip().lower() for t in args.types.split(',') if t.strip()]
            log_type_filter = [settings.analysis_config.supported_log_types.get(t, t) for t in requested_types]
        
        histogram_mode = None
        if args.histogram_mode:
            histogram_mode = [p.strip() for p in args.histogram_mode.split(',') if p.strip()]
        
        return AnalysisConfig(
            start_time=start_time,
            end_time=end_time,
            parallel_threads=args.num_threads,
            histogram_mode=histogram_mode,
            node_filter=node_filter,
            log_type_filter=log_type_filter
        )
    
    def analyze_support_bundle(self, args: argparse.Namespace) -> None:
        """Analyze a support bundle."""
        bundle_path = Path(args.support_bundle)
        if not bundle_path.exists():
            raise ValidationError(f"Support bundle not found: {bundle_path}")
        
        # Check if already analyzed
        bundle_name = bundle_path.stem.replace('.tar', '').replace('.tgz', '')
        existing_report_id = self.database_service.check_report_exists(bundle_name)
        
        if existing_report_id and not args.force:
            self.logger.warning(f"📊 Analysis already completed for support bundle '{bundle_name}'.")
            self.logger.warning(f"🔁 Use --force option to re-trigger the analysis forcefully.")
            self.logger.warning(f"🔗 Use the link below to view the report:")
            report_url = f"http://{settings.server.host}:{settings.server.port}/reports/{existing_report_id}"
            self.logger.warning(report_url)
            self.logger.info(f"🔁 Use --force option to re-trigger the analysis forcefully.")            
            return
        
        # Create analysis configuration
        analysis_config = self.create_analysis_config(args)
        
        # Perform analysis
        self.logger.info(f"Analyzing support bundle: {bundle_name}")
        # self.logger.info(f"Time range: {analysis_config.start_time.strftime('%m%d %H:%M')} to {analysis_config.end_time.strftime('%m%d %H:%M')}")
        
        report = self.analysis_service.analyze_support_bundle(
            bundle_path=bundle_path,
            analysis_config=analysis_config,
            skip_extraction=args.skip_tar
        )
        
        # Save report to file if specified
        if args.output_file:
            output_path = Path(args.output_file)
            self.analysis_service.save_report(report, output_path)
        
        # Store in database and get report URL
        try:
            report_id = self.database_service.store_report(report)
            
            # Generate report URL
            report_url = f"http://{settings.server.host}:{settings.server.port}/reports/{report_id}"
            
            # Create success marker file
            marker_file = bundle_path.parent / f"{bundle_name}.analyzed"
            try:
                with open(marker_file, "w") as f:
                    f.write(report_url + "\n")
                self.logger.info("✅ Success marker file created.")
            except Exception as e:
                self.logger.warning(f"Failed to create marker file: {e}")
                
        except Exception as e:
            self.logger.error(f"👉 Failed to insert report into PostgreSQL: {e}")
            # Still show success for analysis, but warn about database issue
            self.logger.info("✅ Analysis completed successfully!")
            self.logger.warning("⚠️  Report could not be stored in database. Check database connection.")
            return
        
        self.logger.info("✅ Analysis completed successfully!")
        self.logger.info(f"👉 Report available at: {report_url}")
    
    def analyze_parquet_files(self, args: argparse.Namespace) -> None:
        """Analyze Parquet files."""
        import time
        parquet_dir = Path(args.parquet_files)
        if not parquet_dir.exists():
            raise ValidationError(f"Parquet directory not found: {parquet_dir}")
        
        # Get patterns for analysis
        if args.histogram_mode:
            patterns = [p.strip() for p in args.histogram_mode.split(',') if p.strip()]
        else:
            # Use default patterns from configuration
            patterns = self.parquet_service.get_default_patterns()
        
        # Perform analysis with timing
        
        bundle_name = self.parquet_service.get_bundle_name_from_parquet(parquet_dir)
        self.logger.info(f"🚀 Starting Parquet analysis for: {bundle_name}")
        
        existing_report_id = self.database_service.check_report_exists(bundle_name)
        if existing_report_id and not args.force:
            # If report already exists and --force is not used, skip analysis
            self.logger.warning(f"📊 Analysis already completed for Parquet bundle '{bundle_name}'.")
            self.logger.warning(f"🔗 Use the link below to view the report:")
            report_url = f"http://{settings.server.host}:{settings.server.port}/reports/{existing_report_id}"
            self.logger.warning(report_url)
            self.logger.info(f"🔁 Use --force option to re-trigger the analysis forcefully.")
            return
        
        self.logger.info(f"📊 Using {len(patterns)} patterns for analysis")
        
        start_analysis = time.time()
        result = self.parquet_service.analyze_parquet_files(
            parquet_dir=parquet_dir,
            patterns=patterns
        )
        analysis_time = time.time() - start_analysis
        
        # Save results to file
        start_save = time.time()
        if args.output_file:
            output_path = Path(args.output_file)
        else:
            output_path = parquet_dir / "node_log_summary.json"
        
        self.parquet_service.save_results(result, output_path)
        save_time = time.time() - start_save
        
        # Store in database and generate URL
        start_db = time.time()
        try:
            # Create an AnalysisReport from the Parquet results
            from models.log_metadata import AnalysisReport, NodeAnalysisResult, LogMessageStats
            
            # Convert Parquet results to AnalysisReport format
            converted_nodes = {}
            
            for node_name, node_data in result.get("nodes", {}).items():
                converted_nodes[node_name] = {}
                for log_type, log_type_data in node_data.items():
                    node_result = NodeAnalysisResult(
                        node_name=node_name,
                        log_type=log_type,
                        log_messages={}
                    )
                    for pattern_name, message_data in log_type_data.get("logMessages", {}).items():
                        # Robustly handle None and invalid ISO strings
                        def parse_dt(val):
                            if val is None:
                                return datetime.min
                            try:
                                # Accept both ISO and DuckDB string
                                if isinstance(val, str):
                                    # If already ISO, parse
                                    if "T" in val:
                                        # Remove trailing Z if present
                                        val = val.rstrip("Z")
                                        # If no timezone, add +00:00
                                        if "+" not in val and "-" not in val:
                                            val += "+00:00"
                                        return datetime.fromisoformat(val)
                                    else:
                                        # Try DuckDB default format
                                        return datetime.strptime(val[:19], "%Y-%m-%d %H:%M:%S")
                                return val
                            except Exception:
                                return datetime.min
                        log_stats = LogMessageStats(
                            pattern_name=pattern_name,
                            start_time=parse_dt(message_data.get("StartTime")),
                            end_time=parse_dt(message_data.get("EndTime")),
                            count=message_data["count"],
                            histogram=message_data["histogram"]
                        )
                        node_result.log_messages[pattern_name] = log_stats
                    converted_nodes[node_name][log_type] = node_result
            
            # Use actual bundle name from Parquet data
            bundle_name = result.get("universeName", parquet_dir.name)
            
            report = AnalysisReport(
                support_bundle_name=bundle_name,  # Use actual bundle name from data
                nodes=converted_nodes,
                warnings=[
                    {
                        "message": "Parquet file analysis completed",
                        "level": "info",
                        "type": "parquet_analysis"
                    }
                ],
                analysis_config={
                    "parquet_directory": str(parquet_dir),
                    "patterns_used": patterns,
                    "analysis_type": "parquet"
                }
            )
            
            # Store in database
            report_id = self.database_service.store_report(report)
            
            # Generate report URL
            report_url = f"http://{settings.server.host}:{settings.server.port}/reports/{report_id}"
            
            db_time = time.time() - start_db
            
        except Exception as e:
            self.logger.error(f"👉 Failed to insert report into PostgreSQL: {e}")
            self.logger.warning("⚠️  Report could not be stored in database. Check database connection.")
            db_time = time.time() - start_db
        
        total_time = time.time() - start_analysis
        
        self.logger.info(f"🎯 Total execution time: {total_time:.2f} seconds")        
        self.logger.info("✅ Parquet analysis completed successfully!")
        self.logger.info(f"📊 Results saved to: {output_path}")
        
        if 'report_url' in locals():
            self.logger.info(f"👉 Report available at: {report_url}")
    
    def run(self) -> int:
        """Main application entry point."""
        try:
            # Set up argument parser
            parser = self.setup_argument_parser()
            args = parser.parse_args()
            
            # Validate arguments
            self.validate_arguments(args)
            
            # Set up logging
            log_file = None
            if args.support_bundle:
                bundle_path = Path(args.support_bundle)
                log_file = bundle_path.parent / f"{bundle_path.stem}_analyzer.log"
            
            setup_logging(log_file=log_file)
            
            # Run analysis based on input type
            from services.tablet_report_service import TabletReportService
            tablet_report_service = TabletReportService()
            if args.support_bundle:
                # Analyze support bundle and get report_id
                bundle_path = Path(args.support_bundle)
                bundle_name = bundle_path.stem.replace('.tar', '').replace('.tgz', '')
                # Check if already analyzed
                existing_report_id = self.database_service.check_report_exists(bundle_name)
                force = getattr(args, 'force', False)
                if existing_report_id and not force:
                    report_id = existing_report_id
                else:
                    analysis_config = self.create_analysis_config(args)
                    report = self.analysis_service.analyze_support_bundle(
                        bundle_path=bundle_path,
                        analysis_config=analysis_config,
                        skip_extraction=args.skip_tar
                    )
                    if args.output_file:
                        output_path = Path(args.output_file)
                        self.analysis_service.save_report(report, output_path)
                    try:
                        report_id = self.database_service.store_report(report)
                    except Exception as e:
                        self.logger.error(f"👉 Failed to insert report into PostgreSQL: {e}")
                        self.logger.info("✅ Analysis completed successfully!")
                        self.logger.warning("⚠️  Report could not be stored in database. Check database connection.")
                        return 1
                # After support bundle analysis, auto-detect TabletReport dir and process if present
                extracted_dir = bundle_path.parent / bundle_name
                tablet_dir = extracted_dir / "YBA" / "TabletReport"
                if tablet_dir.exists():
                    self.logger.info(f"🚀 Starting Tablet Report analysis for: {tablet_dir}")
                    try:
                        parsed = tablet_report_service.parse(tablet_dir)
                        tablet_report_service.insert_to_db(report_id, parsed)
                        self.logger.info(f"✅ Tablet report data inserted into database with report_id: {report_id}")
                    except Exception as e:
                        self.logger.error(f"❌ Tablet report analysis failed: {e}")
                        return 1
                else:
                    self.logger.info(f"No TabletReport directory found at {tablet_dir}, skipping tablet report analysis.")
            elif args.parquet_files:
                self.analyze_parquet_files(args)
                # If --tablet_report is provided, run tablet report analysis on it
                if args.tablet_report:
                    tablet_dir = Path(args.tablet_report)
                    if not tablet_dir.exists():
                        self.logger.error(f"Tablet report directory not found: {tablet_dir}")
                        return 1
                    self.logger.info(f"🚀 Starting Tablet Report analysis for: {tablet_dir}")
                    try:
                        parsed = tablet_report_service.parse(tablet_dir)
                        import uuid
                        report_id = str(uuid.uuid4())
                        tablet_report_service.insert_to_db(report_id, parsed)
                        self.logger.info(f"✅ Tablet report data inserted into database with report_id: {report_id}")
                    except Exception as e:
                        self.logger.error(f"❌ Tablet report analysis failed: {e}")
                        return 1
            elif args.tablet_report:
                tablet_dir = Path(args.tablet_report)
                if not tablet_dir.exists():
                    self.logger.error(f"Tablet report directory not found: {tablet_dir}")
                    return 1
                self.logger.info(f"🚀 Starting Tablet Report analysis for: {tablet_dir}")
                try:
                    parsed = tablet_report_service.parse(tablet_dir)
                    import uuid
                    report_id = str(uuid.uuid4())
                    tablet_report_service.insert_to_db(report_id, parsed)
                    self.logger.info(f"✅ Tablet report data inserted into database with report_id: {report_id}")
                except Exception as e:
                    self.logger.error(f"❌ Tablet report analysis failed: {e}")
                    return 1
            return 0
            
        except ValidationError as e:
            self.logger.error(f"❌ Validation error: {e}")
            return 1
        except ConfigurationError as e:
            self.logger.error(f"❌ Configuration error: {e}")
            return 1
        except AnalysisError as e:
            self.logger.error(f"❌ Analysis error: {e}")
            return 1
        except LogAnalyzerError as e:
            self.logger.error(f"❌ Application error: {e}")
            return 1
        except KeyboardInterrupt:
            self.logger.info("🛑 Analysis interrupted by user")
            return 1
        except Exception as e:
            self.logger.error(f"❌ Unexpected error: {e}")
            return 1


def main():
    """Main entry point."""
    app = LogAnalyzerApp()
    sys.exit(app.run())


if __name__ == "__main__":
    main() 