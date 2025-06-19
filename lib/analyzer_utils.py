import os
import json
import re
import datetime
from patterns_lib import universe_regex_patterns, pg_regex_patterns
from lib.helper_utils import openLogFile
from lib.log_utils import getTimeFromLog
from tqdm import tqdm
from colorama import just_fix_windows_console

just_fix_windows_console()

# Function to analyze the log files from the nodes
def analyzeNodeLogs(nodeName, logType, subType, startTimeLong, endTimeLong, logFilesMetadata, logger):
    logger.info(f"Analyzing logs for node: {nodeName}, logType: {logType}, subType: {subType}")
    filteredLogs = []
    for logFile, metadata in logFilesMetadata[nodeName][logType][subType].items():
        logStartsAt = datetime.datetime.strptime(metadata["logStartsAt"], '%Y-%m-%d %H:%M:%S')
        logEndsAt = datetime.datetime.strptime(metadata["logEndsAt"], '%Y-%m-%d %H:%M:%S')
        if (logStartsAt >= startTimeLong and logStartsAt <= endTimeLong) or (logEndsAt >= startTimeLong and logEndsAt <= endTimeLong):
            filteredLogs.append(logFile)
    # print(f"Filtered logs for node {nodeName}, logType {logType}, subType {subType}: {len(filteredLogs)} files")
    logger.info(f"Filtered logs: {len(filteredLogs)} files found for node {nodeName}, logType {logType}, subType {subType}")

    # Select patterns and names
    if logType == "postgres":
        patterns = list(pg_regex_patterns.values())
        pattern_names = list(pg_regex_patterns.keys())
    else:
        patterns = list(universe_regex_patterns.values())
        pattern_names = list(universe_regex_patterns.keys())

    # Track per-message stats
    message_stats = {}

    def to_iso(dt):
        if dt is None:
            return None
        return dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Prepare a fixed-width description for alignment
    # Always show logType/subType, truncate nodeName if needed
    max_desc_len = 40
    logtype_str = f"({logType}/{subType})"
    prefix = "Analyzing "
    max_node_len = max_desc_len - len(prefix) - len(logtype_str) - 3  # 3 for ellipsis if needed
    if len(nodeName) > max_node_len:
        node_disp = nodeName[:max_node_len] + "..."
    else:
        node_disp = nodeName
    desc = f"{prefix}{node_disp} {logtype_str}"
    desc = desc.ljust(max_desc_len)

    # Use tqdm progress bar for filteredLogs with only elapsed time and file counts
    with tqdm(
        filteredLogs,
        desc=desc,
        unit="file",
        ncols=None,  # Let tqdm auto-detect terminal width
        bar_format="{l_bar}{bar:80}| {n_fmt}/{total_fmt} files [elapsed: {elapsed}]",
        colour="CYAN"
    ) as pbar:
        for logFile in pbar:
            logger.info(f"Processing log file: {logFile}")
            try:
                with openLogFile(logFile) as logs:
                    if logs is None:
                        continue
                    previousTime = '0101 00:00'
                    for line in logs:
                        try:
                            logTime = getTimeFromLog(line, previousTime)
                            previousTime = logTime.strftime("%m%d %H:%M")
                            if startTimeLong <= logTime <= endTimeLong:
                                for idx, pattern in enumerate(patterns):
                                    if re.search(pattern, line):
                                        msg_type = pattern_names[idx]
                                        hour_bucket = logTime.replace(minute=0, second=0, microsecond=0)
                                        if msg_type not in message_stats:
                                            message_stats[msg_type] = {"StartTime": logTime, "EndTime": logTime, "count": 1, "histogram": {}}
                                        else:
                                            if logTime < message_stats[msg_type]["StartTime"]:
                                                message_stats[msg_type]["StartTime"] = logTime
                                            if logTime > message_stats[msg_type]["EndTime"]:
                                                message_stats[msg_type]["EndTime"] = logTime
                                            message_stats[msg_type]["count"] += 1
                                        # Update histogram
                                        minute_bucket = logTime.replace(second=0, microsecond=0)
                                        minute_key = minute_bucket.strftime('%Y-%m-%dT%H:%M:00Z')
                                        if minute_key not in message_stats[msg_type]["histogram"]:
                                            message_stats[msg_type]["histogram"][minute_key] = 0
                                        message_stats[msg_type]["histogram"][minute_key] += 1
                                        break
                        except ValueError:
                            logger.error(f"Invalid log time format in file {logFile}: {line.strip()}")
                            continue
                        except Exception as e:
                            logger.error(f"Error processing line in file {logFile}: {e}")
                            continue
                    logs.close()
            except Exception as e:
                logger.error(f"Error reading log file {logFile}: {e}")

    # Format times for output
    logMessages = {}
    for msg_type, stats in message_stats.items():
        logMessages[msg_type] = {
            "StartTime": to_iso(stats["StartTime"]),
            "EndTime": to_iso(stats["EndTime"]),
            "count": stats["count"],
            "histogram": stats["histogram"]
        }

    result = {
        "node": nodeName,
        "logType": logType,
        "logMessages": logMessages
    }
    return result

def analyze_log_file_worker(args_tuple):
    nodeName, logType, subType, startTimeLong, endTimeLong, logFilesMetadata, logger = args_tuple
    return analyzeNodeLogs(nodeName, logType, subType, startTimeLong, endTimeLong, logFilesMetadata, logger)

def getUniverseNameFromManifest(logger):
    universeName = "unknown"
    manifestFile = "manifest.json"

    # Search for manifest.json in the current directory and its subdirectories with depth 1
    for root, _, files in os.walk(os.getcwd()):
        if manifestFile in files:
            manifestPath = os.path.join(root, manifestFile)
            with open(manifestPath, 'r') as f:
                manifestData = json.load(f)
                # Extract the universe name from the path
                path = manifestData.get("path", "")
                # Updated regex to allow dashes in universe name
                match = re.search(r'yb-support-bundle-(.+)-\d{14}\.\d+-logs', path)
                if match:
                    universeName = match.group(1)
                logger.info(f"Universe name extracted from manifest: {universeName}")
                break
    else:
        logger.warning(f"manifest.json not found in the current directory or its subdirectories. Using default universe name: {universeName}")
    return universeName
