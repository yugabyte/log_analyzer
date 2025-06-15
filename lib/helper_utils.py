import gzip
import itertools
import sys
import time
import threading

# Function to display the rotating spinner
def spinner(stop_event):
    for c in itertools.cycle(['|', '/', '-', '\\']):
        if stop_event.is_set():
            break
        sys.stdout.write('\r Building the one time log file metadata' + c)
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write('\rDone!     \n')
    
def openLogFile(logFile):
    if logFile.endswith('.gz'):
        try:
            logs = gzip.open(logFile, 'rt')
        except Exception as e:
            logger.error(f"Error opening file {logFile}: {e}")
            return None
    else:
        try:
            logs = open(logFile, 'r')
        except Exception as e:
            logger.error(f"Error opening file {logFile}: {e}")
            return None
    return logs