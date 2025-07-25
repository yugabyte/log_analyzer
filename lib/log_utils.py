import os
import tarfile
import gzip
import re
import datetime
import duckdb
from collections import deque

# Expect logger and args to be passed as parameters to functions that need them

def get_support_bundle_details(args, df=None):
    """
    Get the support bundle name from the args or from the DataFrame if provided
    """
    try:
        if args.support_bundle:
            support_bundle_name = os.path.basename(args.support_bundle).replace('.tar.gz', '').replace('.tgz', '')
            support_bundle_dir = os.path.dirname(args.support_bundle)
            return support_bundle_name, support_bundle_dir
        if args.parquet_files:
            # Query it
            df = df or duckdb.connect().execute(f"SELECT support_bundle,  FROM '{args.parquet_files}/*.parquet' LIMIT 1").df()
            if 'support_bundle' in df.columns:
                support_bundle_name = df['support_bundle'].iloc[0]
                support_bundle_dir = args.parquet_files
                return support_bundle_name, support_bundle_dir
    except Exception as e:
        print(f"Error getting support bundle name: {e}")
        exit(1)

def getArchiveFiles(logDirectory):
    archievedFiles = []
    for root, dirs, files in os.walk(logDirectory):
        for file in files:
            if file.endswith(".tar.gz") or file.endswith(".tgz"):
                archievedFiles.append(os.path.join(root,file))
    return archievedFiles

def getStartAndEndTimes(args):
    # Calculate start and end times
    if args.start_time and args.end_time:
        startTime = datetime.datetime.strptime(args.start_time, '%m%d %H:%M')
        endTime = datetime.datetime.strptime(args.end_time, '%m%d %H:%M')
    elif args.start_time and not args.end_time:
        startTime = datetime.datetime.strptime(args.start_time, '%m%d %H:%M')
        endTime = datetime.datetime.strptime('1231 23:59', '%m%d %H:%M')
    elif not args.start_time and args.end_time:
        startTime = datetime.datetime.strptime('0101 00:00', '%m%d %H:%M')
        endTime = datetime.datetime.strptime(args.end_time, '%m%d %H:%M')
    else:
        startTime = datetime.datetime.strptime('0101 00:00', '%m%d %H:%M')
        endTime = datetime.datetime.strptime('1231 23:59', '%m%d %H:%M')
    startTimeLong = startTime.replace(year=datetime.datetime.now().year)
    endTimeLong = endTime.replace(year=datetime.datetime.now().year)
    startTimeShort = startTime.strftime('%m%d %H:%M')
    endTimeShort = endTime.strftime('%m%d %H:%M')
    return startTimeLong, endTimeLong, startTimeShort, endTimeShort

def extractTarFile(file, logger):
    logger.info("Extracting file {}".format(file))
    with tarfile.open(file, "r:gz") as tar:
        # extract to filename directory
        tar.extractall(os.path.dirname(file))

# Function to extract all the tar files    
def extractAllTarFiles(logDirectory, logger, log_file=None):
    extractedFiles = []
    extractedAll = False
    while not extractedAll:
        extractedAll = True
        for file in getArchiveFiles(logDirectory):
            extractedAll = False
            if file not in extractedFiles:
                logger.info("Extracting file {}".format(file))
                with tarfile.open(file, "r:gz") as tar:
                    try:
                        tar.extractall(os.path.dirname(file))
                    except EOFError:
                        if log_file:
                            logger.warning("Got EOF Exception while extracting file {}, File might have still extracted. Please check {} for more information ".format(file, log_file))
                        logger.error("EOF Exception while extracting file {}".format(file))
                extractedFiles.append(file)
        if len(extractedFiles) >= len(getArchiveFiles(logDirectory)):
            extractedAll = True

# Function to get log files from the current directory
def getLogFilesFromCurrentDir():
    logFiles = []
    logDirectory = os.getcwd()
    for root, dirs, files in os.walk(logDirectory):
        for file in files:
            if file.__contains__("log") and file[0] != ".":
                logFiles.append(os.path.join(root, file))
    return logFiles

def getTimeFromLog(line, previousTime):
    if line[0] in ['I','W','E','F']:
        try:
            timeFromLogStr = line.split(" ")[0][1:] + " " + line.split(" ")[1][:5]
            timestamp = datetime.datetime.strptime(timeFromLogStr, "%m%d %H:%M")
            timestamp = timestamp.replace(year=datetime.datetime.now().year)
        except Exception as e:
            timestamp = datetime.datetime.strptime(previousTime, "%m%d %H:%M")
            timestamp = timestamp.replace(year=datetime.datetime.now().year)
    else:
        try:
            timeFromLogStr = line.split(" ")[0] + " " + line.split(" ")[1]
            timestamp = datetime.datetime.strptime(timeFromLogStr, "%Y-%m-%d %H:%M:%S.%f")
            timestamp = timestamp.strftime("%m%d %H:%M")
            timestamp = datetime.datetime.strptime(timestamp, "%m%d %H:%M")
            timestamp = timestamp.replace(year=datetime.datetime.now().year)
        except Exception as e:
            timestamp = datetime.datetime.strptime(previousTime, "%m%d %H:%M")
            timestamp = timestamp.replace(year=datetime.datetime.now().year)
    return timestamp

def getFileMetadata(logFile, logger):
    logStartsAt, logEndsAt = None, None
    if logFile.endswith('.gz'):
        try:
            logs = gzip.open(logFile, 'rt')
        except:
            print("Error opening file: " + logFile)
            return None
    else:
        try:
            logs = open(logFile, 'r')
        except:
            print("Error opening file: " + logFile)
            return None
    try:
        # Read first 10 lines to get the start time
        for i in range(10):
            line = logs.readline()
            try:
                logStartsAt = getTimeFromLog(line, '0101 00:00')
                break
            except ValueError:
                continue
        # Read last 10 lines to get the end time
        last_lines = deque(logs, maxlen=10)
        for line in reversed(last_lines):
            try:
                logEndsAt = getTimeFromLog(line, '1231 23:59')
                break
            except ValueError:
                continue
    except Exception as e:
        print(f"Error processing file: {logFile} - {e}")
    
    if logStartsAt is None:
        logStartsAt = datetime.datetime.strptime('0101 00:00', '%m%d %H:%M')
    if logEndsAt is None:
        logEndsAt = datetime.datetime.strptime('1231 23:59', '%m%d %H:%M')
    try:
        logStartsAt = logStartsAt.replace(year=datetime.datetime.now().year)
        logEndsAt = logEndsAt.replace(year=datetime.datetime.now().year)
    except Exception as e:
        print("Error getting metadata for file: " + logFile + " " + str(e))
    
    # Get the log type
    if "postgres" in logFile:
        logType = "postgres"
    elif "controller" in logFile:
        logType = "yb-controller"
    elif "tserver" in logFile:
        logType = "yb-tserver"
    elif "master" in logFile:
        logType = "yb-master"
    elif "application" in logFile:
        logType = "YBA"
    else:
        logType = "unknown"
        
    # Get the subtype if available
    if "INFO" in logFile:
        subType = "INFO"
    elif "WARN" in logFile:
        subType = "WARN"
    elif "ERROR" in logFile:
        subType = "ERROR"
    elif "FATAL" in logFile:
        subType = "FATAL"
    elif "postgres" in logFile:
        subType = "INFO"
    elif "application" in logFile:
        subType = "INFO"
    else:
        subType = "unknown"
        
    # Get the node name
    nodeNameRegex = r"/(yb-[^/]*n\d+|yb-(master|tserver)-\d+_[^/]+)/"
    nodeName = re.search(nodeNameRegex, logFile)
    if nodeName:
        nodeName = nodeName.group().replace("/","")
    else:
        nodeName = "unknown"
    
    if logger:
        logger.debug(f"Metadata for file: {logFile} - {logStartsAt} - {logEndsAt} - {logType} - {nodeName} - {subType}")
    return {"logStartsAt": logStartsAt, "logEndsAt": logEndsAt, "logType": logType, "nodeName": nodeName , "subType": subType}

def getLogFilesToBuildMetadata(args, logger, log_file=None):
    logFiles = []
    # Removed directory support, only support_bundle is supported
    if args.support_bundle:
        extractedDir = None
        if args.support_bundle.endswith(".tar.gz") or args.support_bundle.endswith(".tgz"):
            if not args.skip_tar:
                extractTarFile(args.support_bundle, logger)
                extractedDir = args.support_bundle.replace(".tar.gz", "").replace(".tgz", "")
                # Extract the tar files in extracted directory
                extractAllTarFiles(extractedDir, logger, log_file)
            for root, dirs, files in os.walk(extractedDir):
                for file in files:
                    if ("INFO" in file or "postgres" in file) and file[0] != ".":
                        full_path = os.path.abspath(os.path.join(root, file))
                        # Append the files with the absolute path
                        logFiles.append(full_path)
        else:
            logger.error("Invalid support bundle file format. Please provide a .tar.gz or .tgz file")
            exit(1)
    return logFiles

def extract_node_info_from_logs(logFilesMetadata, logger):
    """
    For each node, extract node info (name, master UUID, tserver UUID, hostname, IP) from INFO logs.
    Returns: { nodeName: { ...info... } }
    """
    import re, gzip
    def parse_node_info_line(line):
        m = re.search(r"Node information: \{ hostname: '([^']+)', rpc_ip: '([^']+)', webserver_ip: '([^']+)', uuid: '([^']+)' \}", line)
        if m:
            return {
                'hostname': m.group(1),
                'ip': m.group(2),
                'uuid': m.group(4)
            }
        return None
    node_infos = {}
    for node, logTypes in logFilesMetadata.items():
        node_info = {'node_name': node, 'master_uuid': None, 'tserver_uuid': None, 'hostname': None, 'ip_address': None}
        tserver_info = None
        master_info = None
        tserver_logs = logTypes.get('yb-tserver', {}).get('INFO', {})
        for logFile in tserver_logs:
            try:
                opener = gzip.open if logFile.endswith('.gz') else open
                with opener(logFile, 'rt', errors='ignore') as f:
                    for line in f:
                        info = parse_node_info_line(line)
                        if info:
                            tserver_info = info
                            break
                if tserver_info:
                    break
            except Exception as e:
                logger.debug(f"Failed to parse tserver log {logFile}: {e}")
        master_logs = logTypes.get('yb-master', {}).get('INFO', {})
        for logFile in master_logs:
            try:
                opener = gzip.open if logFile.endswith('.gz') else open
                with opener(logFile, 'rt', errors='ignore') as f:
                    for line in f:
                        info = parse_node_info_line(line)
                        if info:
                            master_info = info
                            break
                if master_info:
                    break
            except Exception as e:
                logger.debug(f"Failed to parse master log {logFile}: {e}")
        if tserver_info:
            node_info['tserver_uuid'] = tserver_info['uuid']
            node_info['hostname'] = tserver_info['hostname']
            node_info['ip_address'] = tserver_info['ip']
        if master_info:
            node_info['master_uuid'] = master_info['uuid']
            if not tserver_info:
                node_info['hostname'] = master_info['hostname']
                node_info['ip_address'] = master_info['ip']
        node_infos[node] = node_info
    return node_infos

def count_tablets_per_tserver(logFilesMetadata):
    """
    For each node, count the number of tablet-meta files in <node_dir>/tserver/tablet-meta/.
    Returns: { nodeName: tablet_count }
    """
    import os, re
    tablet_counts = {}
    uuid_pattern = re.compile(r'^[a-f0-9]{32}$')
    for node, logTypes in logFilesMetadata.items():
        # Find any tserver log to get node_dir
        tserver_logs = logTypes.get('yb-tserver', {}).get('INFO', {})
        node_dir = None
        for logFile in tserver_logs:
            m = re.search(r"(.*/%s/)" % re.escape(node), logFile)
            if m:
                node_dir = m.group(1)
                break
        if not node_dir:
            tablet_counts[node] = 0
            continue
        tablet_meta_dir = os.path.join(node_dir, 'tserver', 'tablet-meta')
        count = 0
        if os.path.isdir(tablet_meta_dir):
            for fname in os.listdir(tablet_meta_dir):
                if uuid_pattern.match(fname):
                    count += 1
        tablet_counts[node] = count
    return tablet_counts

def check_tserver_log_utc(logFilesMetadata, logger=None):
    """
    Check if any tserver INFO log is not in UTC by reading the first two lines.
    Returns a warning dict if not UTC, else None.
    """
    for node, logTypes in logFilesMetadata.items():
        tserver_logs = logTypes.get('yb-tserver', {}).get('INFO', {})
        for logFile in tserver_logs:
            try:
                opener = gzip.open if logFile.endswith('.gz') else open
                with opener(logFile, 'rt', errors='ignore') as f:
                    first = f.readline().strip()
                    second = f.readline().strip()
                    # Look for the expected lines
                    if first.startswith('Log file created at:') and second.startswith('Current UTC time:'):
                        # Extract times
                        t1 = first.split('Log file created at:')[1].strip()
                        t2 = second.split('Current UTC time:')[1].strip()
                        if t1 != t2:
                            return {
                                'type': 'log_timezone',
                                'level': 'warning',
                                'message': 'TServer logs are not in UTC.',
                                'node': node,
                                'file': logFile,
                                'additional_details': 'Log file created at: {} | Current UTC time: {}'.format(t1, t2)
                            }
                        else:
                            return None  # Found a UTC log, no warning
            except Exception as e:
                if logger:
                    logger.debug(f"Failed to check UTC for {logFile}: {e}")
                continue
    return None  # No tserver INFO log found or all are UTC

def collect_report_warnings(logFilesMetadata, logger=None):
    """
    Collect all warnings for the report. Returns a list of warning dicts.
    """
    warnings = []
    utc_warn = check_tserver_log_utc(logFilesMetadata, logger)
    if utc_warn:
        warnings.append(utc_warn)
    # Add more warning checks here in the future
    return warnings
