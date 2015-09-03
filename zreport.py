#!/usr/bin/python

######################################################################################################
# created by Bukhal Peter <bukhal at i-teco.ru>
######################################################################################################

import os
import time
import logging
import logging.handlers
import glob
import sys

# Paths configuration
ZREPORT_DIRECTORY="data"
ZREPORT_DIRECTORY_CHECK_INTERVAL=3
ZREPORT_ARCHIVE_DIRECTORY="archive"

# Task configuration
TASK_PID=str(os.getpid())
TASK_LOCK_FILE="zreport.lock"
TASK_TOTAL_EXECUTION_TIME=""
TASK_MAX_EXECUTION_TIME=10 # In seconds
TASK_ALLOW_MULTIPLE_INSTANSES=False

# Logging configuration
LOG_DIR="log/"
LOG_FILE=LOG_DIR+"zreport.log"
LOG_FILE_MAX_SIZE=1024 * 1024 * 2 # 2Mb
LOG_FILE_BACKUP_COUNT=5

# FTP connection configuration
FTP_USERNAME="zreport"
FTP_PASSWORD="QAwKySdTwk"

# Create log folder if it not exists
if not os.path.isdir(LOG_DIR):
    os.mkdir(LOG_DIR)

logger = logging.getLogger("zReport")
logger.setLevel("INFO")
defaultFormatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s {pid=%(process)d}: %(message)s")

fileHandler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=LOG_FILE_MAX_SIZE, backupCount=LOG_FILE_BACKUP_COUNT)
fileHandler.setFormatter(defaultFormatter)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(defaultFormatter)

logger.addHandler(fileHandler)
logger.addHandler(consoleHandler)

# Checks if execution time exceeded
def time_exceeded():
    diff = int(time.time() - os.path.getctime(TASK_LOCK_FILE))

    if diff > TASK_MAX_EXECUTION_TIME:
        return True
    else:
        return False

#
def send_report_to_server(report):
    logger.info("Sending report " + report)

#
def wait_for_reports():
    logger.info("Waiting for reports... (check it every ~" + str(ZREPORT_DIRECTORY_CHECK_INTERVAL) + " seconds)")
    while True:
        for report in glob.glob(ZREPORT_DIRECTORY + "/ZReport*.xml"):
            send_report_to_server(os.path.basename(report))

        if time_exceeded():
            logger.info("Task has exceeded max execution time and will be stopped normally")

            return
        else:
            time.sleep(ZREPORT_DIRECTORY_CHECK_INTERVAL)

#
def check_pid(pid):
    try:
        os.kill(int(pid), 9)
    except OSError:
        return False
    else:
        return True

#
def check_lock_file():
    if os.path.isfile(TASK_LOCK_FILE):
        with open(TASK_LOCK_FILE,"r") as lock_file:
            lock_pid = lock_file.readline()

        logger.warn("Task w/ pid " + lock_pid + " already running")
        while True:
            if os.path.isfile(TASK_LOCK_FILE):
                diff = int(time.time() - os.path.getctime(TASK_LOCK_FILE))

                if diff > TASK_MAX_EXECUTION_TIME:
                    logger.warn("Task w/ pid " + lock_pid + " has exceeded max execution time and will be terminated")

                    if check_pid(lock_pid):
                        logger.warn("Task w/ pid " + lock_pid + " was successfully terminated")
                    else:
                        logger.warn("Task process w/ pid " + lock_pid + " was not found, skip task termination")

                    remove_lock_file()

                    return True
                else:
                    # If another tast is running and TASK_ALLOW_MULTIPLE_INSTANSES enabled
                    # wait for another task to complete else stop itself
                    if TASK_ALLOW_MULTIPLE_INSTANSES:
                        logger.warn("wait for 5 seconds...")
                        time.sleep(5)
                    else:
                        logger.warn("Multiple tasks execution is disabled, exiting...")
                        return False
            else:
                logger.warn("Task w/ pid " + lock_pid + " has completed, resume...")
                return True
    else:
        return True

#
def create_lock_file():
    with open(TASK_LOCK_FILE,"w") as lock_file:
        lock_file.write(TASK_PID)

#
def remove_lock_file():
    if os.path.isfile(TASK_LOCK_FILE):
        os.remove(TASK_LOCK_FILE)

#
def init():
    if check_lock_file():
        create_lock_file()
        logger.info("Started")
    else:
        sys.exit(0)

#
def finish():
    remove_lock_file()
    logger.info("Completed")

init()

wait_for_reports()

finish()