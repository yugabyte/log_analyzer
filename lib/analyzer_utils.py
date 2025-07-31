import os
import json
import re
import datetime
from lib.patterns_lib import universe_regex_patterns, pg_regex_patterns
from lib.helper_utils import openLogFile
from lib.log_utils import getTimeFromLog
from tqdm import tqdm
from colorama import just_fix_windows_console

just_fix_windows_console()

# Function to analyze the log files from the nodes
def analyzeNodeLogs(nodeName, logType, subType, startTimeLong, endTimeLong, logFilesMetadata, logger, position=0, histogram_mode=None):
    logger.debug(f"Analyzing logs for node: {nodeName}, logType: {logType}, subType: {subType}")
    filteredLogs = []
    for logFile, metadata in logFilesMetadata[nodeName][logType][subType].items():
        logStartsAt = datetime.datetime.strptime(metadata["logStartsAt"], '%Y-%m-%d %H:%M:%S')
        logEndsAt = datetime.datetime.strptime(metadata["logEndsAt"], '%Y-%m-%d %H:%M:%S')
        if (logStartsAt >= startTimeLong and logStartsAt <= endTimeLong) or (logEndsAt >= startTimeLong and logEndsAt <= endTimeLong):
            filteredLogs.append(logFile)
    logger.debug(f"Filtered logs: {len(filteredLogs)} files found for node {nodeName}, logType {logType}, subType {subType}")

    # If histogram_mode is set, use only those patterns (custom or named), else use defaults
    if histogram_mode:
        # histogram_mode can be a comma-separated list of patterns (regexes)
        custom_patterns = [p.strip() for p in histogram_mode.split(",") if p.strip()]
        patterns = custom_patterns
        pattern_names = custom_patterns
    else:
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
    # Show only last 40 chars of nodeName if longer, and only first 8 chars of logType
    node_disp = nodeName[-40:] if len(nodeName) > 40 else nodeName
    logtype_disp = logType[:8]
    logtype_str = f"({logtype_disp}/{subType})"
    desc = f"{node_disp} {logtype_str}"
    desc = desc.ljust(50)

    # Use tqdm progress bar for filteredLogs with only elapsed time and file counts
    # Only analyze if filteredLogs is not empty
    if filteredLogs:
        with tqdm(
            filteredLogs,
            desc=desc,
            unit="file",
            ncols=None,  # Let tqdm auto-detect terminal width
            bar_format="{l_bar}{bar:80}| {n_fmt}/{total_fmt} files [elapsed: {elapsed}]",
            colour="CYAN",
            position=position
        ) as pbar:
            for logFile in pbar:
                logger.debug(f"Processing log file: {logFile}")
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
    # Unpack with position and histogram_mode
    if len(args_tuple) == 9:
        nodeName, logType, subType, startTimeLong, endTimeLong, logFilesMetadata, logger, position, histogram_mode = args_tuple
        return analyzeNodeLogs(nodeName, logType, subType, startTimeLong, endTimeLong, logFilesMetadata, logger, position=position, histogram_mode=histogram_mode)
    else:
        nodeName, logType, subType, startTimeLong, endTimeLong, logFilesMetadata, logger, position = args_tuple
        return analyzeNodeLogs(nodeName, logType, subType, startTimeLong, endTimeLong, logFilesMetadata, logger, position=position)
