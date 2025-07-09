from lib.analyzer_utils import analyzeNodeLogs, analyze_log_file_worker, getUniverseNameFromManifest
from lib.helper_utils import spinner, openLogFile
from lib.log_utils import (
    getFileMetadata,
    getLogFilesToBuildMetadata,
    getStartAndEndTimes,
    extractAllTarFiles,
    extractTarFile,
    getArchiveFiles,
    getLogFilesFromCurrentDir,
    getTimeFromLog,
    get_gflags_from_nodes,
    extract_node_info_from_logs,
    count_tablets_per_tserver
)
from multiprocessing import Pool, Lock, Manager
from colorama import Fore, Style
from patterns_lib import (
    universe_regex_patterns,
    pg_regex_patterns,
)
from collections import deque
import logging
import datetime
import argparse
import re
import os
import tabulate
import tarfile
import gzip
import json
import sys
import itertools
import time
import threading
from tqdm import tqdm

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

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:- %(message)s')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logFile = "analyzer.log"
file_handler = logging.FileHandler(logFile)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

if __name__ == "__main__":
    logFilesMetadata = {}
    logFilesMetadataFile = 'log_files_metadata.json'
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
    # Get long and short start and end times
    startTimeLong, endTimeLong, startTimeShort, endTimeShort = getStartAndEndTimes(args)
    logger.info(f"Analyzing logs from {startTimeShort} to {endTimeShort}")
    # Prepare tasks for parallel processing
    tasks = []
    for idx, (nodeName, nodeData) in enumerate(logFilesMetadata.items()):
        for logType, logTypeData in nodeData.items():
            for subType, subTypeData in logTypeData.items():
                tasks.append((nodeName, logType, subType, startTimeLong, endTimeLong, logFilesMetadata, logger, idx, args.histogram_mode))
    # Get the universe name from manifest.json
    universeName = getUniverseNameFromManifest(logger)
    logger.info(f"Universe name: {universeName}")

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
    # --- Add GFlags from server.conf for all nodes (top-level) ---
    gflags = get_gflags_from_nodes(logFilesMetadata)
    # --- End GFlags addition ---

    # --- Node info extraction ---
    node_infos = extract_node_info_from_logs(logFilesMetadata, logger)
    # --- End node info extraction ---

    # --- Tablet count extraction ---
    tablet_counts = count_tablets_per_tserver(logFilesMetadata)
    # --- End tablet count extraction ---

    # Write nested results to a JSON file, including universeName and top-level GFlags
    output_json = {
        "universeName": universeName,
        "GFlags": gflags if gflags else {},
        "nodes": nested_results
    }
    # Add node_info metadata and tablet count under each node
    for node, info in node_infos.items():
        if node in output_json["nodes"]:
            output_json["nodes"][node]["node_info"] = info
        else:
            output_json["nodes"][node] = {"node_info": info}
        # Add tablet count under node_info
        output_json["nodes"][node]["node_info"]["tablet_count"] = tablet_counts.get(node, 0)
    with open("node_log_summary.json", "w") as f:
        json.dump(output_json, f, indent=2)
    logger.info("Wrote node log summary to node_log_summary.json")
    logger.info("Log analysis completed.")

    # --- Insert report into PostgreSQL ---
    import psycopg2
    from psycopg2.extras import Json
    try:
        # Read DB config from db_config.json (always from script directory)
        config_path = os.path.join(os.path.dirname(__file__), "db_config.json")
        with open(config_path) as config_file:
            db_config = json.load(config_file)
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"]
        )
        cur = conn.cursor()
        with open("node_log_summary.json") as f:
            report_json = json.load(f)
        # Extract support_bundle_name from args.support_bundle (remove .tar.gz or .tgz)
        support_bundle_name = os.path.basename(args.support_bundle) if args.support_bundle else "unknown"
        if support_bundle_name.endswith(".tar.gz"):
            support_bundle_name = support_bundle_name[:-7]
        elif support_bundle_name.endswith(".tgz"):
            support_bundle_name = support_bundle_name[:-4]
        cur.execute(
            """
            INSERT INTO public.reports (id, support_bundle_name, json_report, created_at)
            VALUES (gen_random_uuid(), %s, %s, NOW())
            """,
            (support_bundle_name, Json(report_json))
        )
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Report inserted into public.reports table.")
    except Exception as e:
        logger.error(f"Failed to insert report into PostgreSQL: {e}")