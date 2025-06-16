import os
import tarfile
import gzip
import re
import datetime
from collections import deque

# Expect logger and args to be passed as parameters to functions that need them

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
        return None
    
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
    if args.directory:
        if not args.skip_tar:
            extractAllTarFiles(args.directory, logger, log_file)
        for root, dirs, files in os.walk(args.directory):
            for file in files:
                if ("INFO" in file or "postgres" in file) and file[0] != ".":
                    logFiles.append(os.path.join(root, file))
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

def get_gflags_from_nodes(logFilesMetadata):
    """
    For all nodes, find tserver/master server.conf, parse, and return a dict with keys 'tserver' and/or 'master'.
    If GFlags are the same for all nodes, just return the first found for each process.
    """
    import os
    import re
    def parse_server_conf(conf_path):
        gflags = {}
        try:
            with open(conf_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or not line.startswith('--'):
                        continue
                    keyval = line[2:].split('=', 1)
                    if len(keyval) == 2:
                        key, val = keyval
                        if val.lower() == 'true':
                            val = True
                        elif val.lower() == 'false':
                            val = False
                        else:
                            try:
                                val = int(val)
                            except ValueError:
                                try:
                                    val = float(val)
                                except ValueError:
                                    pass
                        gflags[key] = val
            return gflags
        except Exception:
            return None
    gflags = {}
    found = {'tserver': False, 'master': False}
    for nodeName in logFilesMetadata:
        found_log = None
        for logType in logFilesMetadata[nodeName]:
            for subType in logFilesMetadata[nodeName][logType]:
                for logFile in logFilesMetadata[nodeName][logType][subType]:
                    found_log = logFile
                    break
                if found_log:
                    break
            if found_log:
                break
        if not found_log:
            continue
        m = re.search(r"(.*/%s/)" % re.escape(nodeName), found_log)
        if not m:
            continue
        node_dir = m.group(1)
        # tserver
        if not found['tserver']:
            tserver_conf = os.path.join(node_dir, 'tserver', 'conf', 'server.conf')
            if os.path.isfile(tserver_conf):
                tserver_flags = parse_server_conf(tserver_conf)
                if tserver_flags:
                    gflags['tserver'] = tserver_flags
                    found['tserver'] = True
        # master
        if not found['master']:
            master_conf = os.path.join(node_dir, 'master', 'conf', 'server.conf')
            if os.path.isfile(master_conf):
                master_flags = parse_server_conf(master_conf)
                if master_flags:
                    gflags['master'] = master_flags
                    found['master'] = True
        if all(found.values()):
            break
    return gflags if gflags else None
