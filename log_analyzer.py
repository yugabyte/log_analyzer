from lib.analyzer_utils import analyze_log_file_worker
from lib.helper_utils import spinner
from lib.log_utils import (
    getFileMetadata,
    getLogFilesToBuildMetadata,
    getStartAndEndTimes,
    get_support_bundle_details,
    collect_report_warnings
)
from multiprocessing import Pool
from colorama import Fore, Style
from lib.patterns_lib import (
    universe_regex_patterns,
    pg_regex_patterns,
)
import psycopg2
from psycopg2.extras import Json
import logging
import datetime
import argparse
import uuid
import os
import json
import threading
from lib.parquet_lib import analyzeParquetFiles

class ColoredHelpFormatter(argparse.RawTextHelpFormatter):
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

# Command line arguments
parser = argparse.ArgumentParser(description="Log Analyzer for YugabyteDB logs", formatter_class=ColoredHelpFormatter)
parser.add_argument("-s","--support_bundle", help="Support bundle file name")
parser.add_argument("--parquet_files", metavar="DIR", help="Directory containing Parquet files to analyze")
parser.add_argument("--types", metavar="LIST", help="List of log types to analyze \n Example: --types 'ms,ybc' \n Default: --types 'pg,ts,ms'")
parser.add_argument("-n", "--nodes", metavar="LIST", help="List of nodes to analyze \n Example: --nodes 'n1,n2'")
parser.add_argument("-o", "--output", metavar="FILE", dest="output_file", help="Output file name")
parser.add_argument("-p", "--parallel", metavar="N", dest='numThreads', default=5, type=int, help="Run in parallel mode with N threads")
parser.add_argument("--skip_tar", action="store_true", help="Skip tar file")
parser.add_argument("-t", "--from_time", metavar= "MMDD HH:MM", dest="start_time", help="Specify start time in quotes")
parser.add_argument("-T", "--to_time", metavar= "MMDD HH:MM", dest="end_time", help="Specify end time in quotes")
parser.add_argument("--histogram-mode", dest="histogram_mode", metavar="LIST", help="List of errors to generate histogram \n Example: --histogram-mode 'error1,error2,error3'")
args = parser.parse_args()

# Validated start and end time format
if args.start_time:
    try:
        datetime.datetime.strptime(args.start_time, "%m%d %H:%M")
    except ValueError as e:
        print("Incorrect start time format, should be MMDD HH:MM")
        exit(1)
if args.end_time:
    try:
        datetime.datetime.strptime(args.end_time, "%m%d %H:%M")
    except ValueError as e:
        print("Incorrect end time format, should be MMDD HH:MM")
        exit(1)

# 7 days ago from today
seven_days_ago = datetime.datetime.now() - datetime.timedelta(days=7)
seven_days_ago = seven_days_ago.strftime("%m%d %H:%M")
# If not start time then set it to today - 7 days in "MMDD HH:MM" format
start_time = datetime.datetime.strptime(args.start_time, "%m%d %H:%M") if args.start_time else datetime.datetime.strptime(seven_days_ago, "%m%d %H:%M")
end_time = datetime.datetime.strptime(args.end_time, "%m%d %H:%M") if args.end_time else datetime.datetime.now()

reportJSON = {}
support_bundle_name, support_bundle_dir = get_support_bundle_details(args)
        

# Set up logging

logFile = os.path.join(support_bundle_dir, support_bundle_name + '_analyzer.log')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:- %(message)s')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

file_handler = logging.FileHandler(logFile)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# --- Prevent duplicate analysis ---
success_marker_file = os.path.join(support_bundle_dir, f"{support_bundle_name}.analyzed")
if os.path.exists(success_marker_file):
    logger.warning(f"Analysis already completed for support bundle '{support_bundle_name}'. Skipping.")
    logger.warning(f"Use the link below to view the report:")
    with open(success_marker_file, "r") as f:
        report_link = f.read().strip()
        logger.warning(report_link)
    logger.warning("If you want to re-analyze the logs, please remove the marker file:")
    logger.warning(f"{success_marker_file}")
    exit(0)


def postProcess(report_json, support_bundle_name, logger, success_marker_file):
    try:
        db_config_path = os.path.join(os.path.dirname(__file__), "db_config.json")
        with open(db_config_path) as db_config_file:
            db_config = json.load(db_config_file)
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        cur = conn.cursor()
        random_id = os.urandom(16).hex()
        cur.execute(
            """
            INSERT INTO public.log_analyzer_reports (id, support_bundle_name, json_report, created_at)
            VALUES (%s, %s, %s, NOW())
            """,
            (random_id, support_bundle_name, Json(report_json))
        )
        conn.commit()
        cur.close()
        conn.close()
        print("")
        print("")
        logger.info("👉 Report inserted into public.log_analyzer_reports table.")
        server_config_path = os.path.join(os.path.dirname(__file__), "server_config.json")
        with open(server_config_path) as server_config_file:
            server_config = json.load(server_config_file)
        host = server_config.get("host", "127.0.0.1")
        port = server_config.get("port", 5000)
        report_url = f"http://{host}:{port}/reports/{str(uuid.UUID(random_id))}"
        logger.info(f"👉 ⌘ + click to open your report at: {report_url}")
        try:
            with open(success_marker_file, "w") as f:
                f.write(report_url + "\n")
                logger.info("Success marker file created.")
        except Exception as e:
            logger.warning(f"Failed to create marker file: {e}")
    except Exception as e:
        logger.error(f"👉 Failed to insert report into PostgreSQL: {e}")

if __name__ == "__main__":
    logFilesMetadata = {}
    logFilesMetadataFile = os.path.join(support_bundle_dir, support_bundle_name + '_log_files_metadata.json')
    nodeLogSummaryFile = os.path.join(support_bundle_dir, support_bundle_name + '_node_log_summary.json')
    logger.info(f"Analyzing support bundle: {support_bundle_name} from {support_bundle_dir}")
    output_json = None
    # --- Support bundle analysis ---
    if args.support_bundle:
        if os.path.exists(logFilesMetadataFile):
            logger.info(f"Loading log files metadata from {logFilesMetadataFile}")
            with open(logFilesMetadataFile, 'r') as f:
                logFilesMetadata = json.load(f)
        else:
            # Only support_bundle is supported now
            logFileList = getLogFilesToBuildMetadata(args, logger, logFile)
            if not logFileList:
                logger.error("No log files found in the specified support bundle.")
                exit(1)
            done = False
    
            # Start the spinner in a separate thread
            stop_event = threading.Event()
            spinner_thread = threading.Thread(target=spinner, args=(stop_event,))
            spinner_thread.start()
    
            # Build the metadata for the log files
            for logFile in logFileList:
                metadata = getFileMetadata(logFile, logger)
                if metadata:
                    node = metadata["nodeName"]
                    logType = metadata["logType"]
                    subType = metadata["subType"]
                    if node not in logFilesMetadata:
                        logFilesMetadata[node] = {}
                    if logType not in logFilesMetadata[node]:
                        logFilesMetadata[node][logType] = {}
                    if subType not in logFilesMetadata[node][logType]:
                        logFilesMetadata[node][logType][subType] = {}
                    logFilesMetadata[node][logType][subType][logFile] = {
                        "logStartsAt": str(metadata["logStartsAt"]),
                        "logEndsAt": str(metadata["logEndsAt"])
                    }
            # Save the metadata to a file
            with open(logFilesMetadataFile, 'w') as f:
                json.dump(logFilesMetadata, f, indent=4)
            stop_event.set()
            spinner_thread.join()
            logger.info(f"Log files metadata saved to {logFilesMetadataFile}")

        # --- Filter nodes if --nodes is specified ---
        if args.nodes:
            requested_nodes = [n.strip().lower() for n in args.nodes.split(',') if n.strip()]
            filtered_logFilesMetadata = {}
            for node_name, node_data in logFilesMetadata.items():
                if any(req in node_name.lower() for req in requested_nodes):
                    filtered_logFilesMetadata[node_name] = node_data
            logFilesMetadata = filtered_logFilesMetadata
            if not logFilesMetadata:
                logger.error(f"No matching nodes found for --nodes: {args.nodes}")
                logger.info(f"Available nodes: {list(logFilesMetadata.keys())}")
                exit(1)
    
        # --- Filter log types if --types is specified ---
        type_map = {
            'pg': 'postgres',
            'ts': 'yb-tserver',
            'ms': 'yb-master',
            'ybc': 'yb-controller',
        }
        if args.types:
            requested_types = [t.strip().lower() for t in args.types.split(',') if t.strip()]
            expanded_types = [type_map.get(t, t) for t in requested_types]
            filtered_logFilesMetadata = {}
            for node_name, node_data in logFilesMetadata.items():
                filtered_node_data = {log_type: log_type_data for log_type, log_type_data in node_data.items() if log_type in expanded_types}
                if filtered_node_data:
                    filtered_logFilesMetadata[node_name] = filtered_node_data
            logFilesMetadata = filtered_logFilesMetadata
            if not logFilesMetadata:
                logger.error(f"No matching log types found for --types: {args.types}")
                exit(1)
        # Get long and short start and end times
        startTimeLong, endTimeLong, startTimeShort, endTimeShort = getStartAndEndTimes(args)
        logger.info(f"Analyzing logs from {startTimeShort} to {endTimeShort}")
        # Prepare tasks for parallel processing
        tasks = []
        for idx, (nodeName, nodeData) in enumerate(logFilesMetadata.items()):
            for logType, logTypeData in nodeData.items():
                for subType, subTypeData in logTypeData.items():
                    tasks.append((nodeName, logType, subType, startTimeLong, endTimeLong, logFilesMetadata, logger, idx, args.histogram_mode))
        # Analyze in parallel
        with Pool(processes=args.numThreads) as pool:
            results = pool.map(analyze_log_file_worker, tasks)
        # Build nested result: node -> logType -> logMessages
        nested_results = {}
        for result in results:
            nodeName = result["node"]
            logType = result["logType"]
            if nodeName not in nested_results:
                nested_results[nodeName] = {}
            if logType not in nested_results[nodeName]:
                nested_results[nodeName][logType] = {"logMessages": {}}
            for msg, stats in result["logMessages"].items():
                nested_results[nodeName][logType]["logMessages"][msg] = stats
    
        # Write nested results to a JSON file (remove GFlags from output)
        output_json = {
            "nodes": nested_results
        }
    
        # --- Add warnings to the root of the JSON ---
        warnings = collect_report_warnings(logFilesMetadata, logger) or []
        # Add custom options warning if --nodes or --types was used
        custom_opts = []
        if args.nodes:
            custom_opts.append(f"--nodes: {args.nodes}")
        if args.types:
            custom_opts.append(f"--types: {args.types}")
        if custom_opts:
            warnings.append({
                "message": "This report was generated with custom options. So, the results may not include all logs.",
                "additional_details": f"Custom options used: {'; '.join(custom_opts)}",
                "level": "info",
                "type": "custom_options"
            })
        if warnings:
            output_json["warnings"] = warnings
    
        with open(nodeLogSummaryFile, "w") as f:
            json.dump(output_json, f, indent=2)
        logger.info(f"Support bundle analysis complete. Output written to {nodeLogSummaryFile}")

    # --- Parquet files analysis ---
    if args.parquet_files:
        # Check for unsupported options
        unsupported_opts = []
        # Use args.numThreads instead of args.parallel
        if args.types:
            unsupported_opts.append("--types")
        if args.nodes:
            unsupported_opts.append("--nodes")
        if hasattr(args, "numThreads") and args.numThreads != 5:  # default is 5
            unsupported_opts.append("--parallel")
        if args.skip_tar:
            unsupported_opts.append("--skip_tar")
        if args.start_time:
            unsupported_opts.append("--from_time")
        if args.end_time:
            unsupported_opts.append("--to_time")
        if unsupported_opts:
            logger.error(f"The following options are not supported with --parquet_files: {', '.join(unsupported_opts)}")
            exit(1)
        if not os.path.exists(args.parquet_files):
            logger.error(f"Parquet directory '{args.parquet_files}' does not exist.")
            exit(1)
        if args.histogram_mode:
            log_patterns = [p.strip() for p in args.histogram_mode.split(',') if p.strip()]
        else:
            log_patterns = list(universe_regex_patterns.values()) + list(pg_regex_patterns.values())
        nodeLogSummaryFile = os.path.join(support_bundle_dir, support_bundle_name + '_node_log_summary.json')
        result = analyzeParquetFiles(args.parquet_files, log_patterns, output_path=nodeLogSummaryFile)
        output_json = {
            "nodes": result.get("nodes", {}),
            "universeName": result.get("universeName", "Unknown")
        }
        warnings = []
        if args.histogram_mode:
            warnings.append({
                "message": "Histogram mode was used for pattern selection.",
                "level": "info",
                "type": "custom_options"
            })
        if warnings:
            output_json["warnings"] = warnings
        with open(nodeLogSummaryFile, "w") as f:
            json.dump(output_json, f, indent=2)
        logger.info(f"Parquet analysis complete. Output written to {nodeLogSummaryFile}")

    # --- Insert report into PostgreSQL (once, at the end) ---
    if output_json is not None:
        postProcess(output_json, support_bundle_name, logger, success_marker_file)