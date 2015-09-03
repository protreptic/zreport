#!/usr/bin/python

######################################################################################################
# created by Peter Bukhal <bukhal at i-teco.ru>
######################################################################################################

import os
import time
import logging
import logging.handlers
import glob
import sys
import xml.etree.ElementTree as et
import ftplib
import shutil
import zipfile

# Paths configuration
REPORT_DIRECTORY = "data"
REPORT_DIRECTORY_CHECK_INTERVAL = 3
ARCHIVE_DIRECTORY = "archive"

# Task configuration
TASK_PID = str(os.getpid())
TASK_LOCK_FILE = "zreport.lock"
TASK_TOTAL_EXECUTION_TIME = ""
TASK_MAX_EXECUTION_TIME = 10  # In seconds
TASK_ALLOW_MULTIPLE_INSTANCES = False

# Logging configuration
LOG_DIR = "log"
LOG_FILE = LOG_DIR+"/zreport.log"
LOG_FILE_MAX_SIZE = 1024 * 1024 * 2  # 2Mb
LOG_FILE_BACKUP_COUNT = 5

# FTP connection configuration
FTP_SERVER = "localhost"
FTP_SERVER_PATH = "./IdeaProjects/zreport/ftpdest1"
FTP_USERNAME = "peterbukhal"
FTP_PASSWORD = "123"

# Initialize logger
def init_logger():
    # Create log folder if it not exists
    if not os.path.isdir(LOG_DIR):
        os.mkdir(LOG_DIR)

    logger = logging.getLogger("zReport")
    logger.setLevel("INFO")
    default_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s {pid=%(process)d}: %(message)s")

    file_handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=LOG_FILE_MAX_SIZE, backupCount=LOG_FILE_BACKUP_COUNT)
    file_handler.setFormatter(default_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(default_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

main_logger = init_logger()

# Checks if execution time exceeded
def time_exceeded():
    diff = int(time.time() - os.path.getctime(TASK_LOCK_FILE))

    if diff > TASK_MAX_EXECUTION_TIME:
        return True
    else:
        return False

class ZReportDestination:

    def __init__(self, ftp_server, ftp_username, ftp_password, ftp_server_path):
        self.ftp_server = ftp_server
        self.ftp_username = ftp_username
        self.ftp_password = ftp_password
        self.ftp_server_path = ftp_server_path

        self.ftp = ftplib.FTP(self.ftp_server)
        self.ftp.login(self.ftp_username, self.ftp_password)
        self.ftp.cwd(self.ftp_server_path)

    def send_report(self, report):
        # STOR report file to server
        with open(report, "r") as report_file:
            self.ftp.storlines("STOR " + os.path.basename(report), report_file)
            main_logger.info("Report " + os.path.basename(report) + " was successfully sent to destination server")
            archive_report(report)

# Validates reports to be a valid xml document
# and send it to a server destination
def send_report_to_server(report):
    main_logger.info("Sending report " + os.path.basename(report) + "...")

    # validate xml document
    try:
        tree = et.parse(report)
        root = tree.getroot()
    except et.ParseError:
        main_logger.warn("Report " + os.path.basename(report) + " is not well-formed xml document, skip it")
    else:
        # Send report to server
        ftp_destination = ZReportDestination(FTP_SERVER, FTP_USERNAME, FTP_PASSWORD, FTP_SERVER_PATH)
        ftp_destination.send_report(report)

# Move sent reports to archive directory,
# remove from archive directory old reports
def archive_report(report):
    if not os.path.isdir(ARCHIVE_DIRECTORY):
        os.mkdir(ARCHIVE_DIRECTORY)

    archived_report = ARCHIVE_DIRECTORY + "/" + os.path.basename(report)

    with zipfile.ZipFile(ARCHIVE_DIRECTORY + "/archive.zip", "w") as archive:
        shutil.copy(report, archived_report)
        archive.write(archived_report)
        main_logger.info("Report " + os.path.basename(report) + " was successfully archived")

#
def wait_for_reports():
    main_logger.info("Waiting for reports... (check it every ~" + str(REPORT_DIRECTORY_CHECK_INTERVAL) + " seconds)")
    while True:
        for report in glob.glob(REPORT_DIRECTORY + "/ZReport*.xml"):
            send_report_to_server(report)

        if time_exceeded():
            main_logger.info("Task has exceeded max execution time and will be stopped normally")

            return
        else:
            time.sleep(REPORT_DIRECTORY_CHECK_INTERVAL)

# Tries to terminate given process by it`s PID
# if fails returns False
# else if process with PID was terminated returns True
def terminate_pid(pid):
    try:
        os.kill(int(pid), 9)
    except OSError:
        return False
    else:
        return True

#
def check_lock_file():
    if os.path.isfile(TASK_LOCK_FILE):
        with open(TASK_LOCK_FILE, "r") as lock_file:
            lock_pid = lock_file.readline()

        main_logger.warn("Task w/ pid " + lock_pid + " already running")
        while True:
            if os.path.isfile(TASK_LOCK_FILE):
                diff = int(time.time() - os.path.getctime(TASK_LOCK_FILE))

                if diff > TASK_MAX_EXECUTION_TIME:
                    main_logger.warn("Task w/ pid " + lock_pid + " has exceeded max execution time and will be terminated")

                    if terminate_pid(lock_pid):
                        main_logger.warn("Task w/ pid " + lock_pid + " was successfully terminated")
                    else:
                        main_logger.warn("Task process w/ pid " + lock_pid + " was not found, skip task termination")

                    remove_lock_file()

                    return True
                else:
                    # If another task is running and TASK_ALLOW_MULTIPLE_INSTANCES enabled
                    # wait for another task to complete else stop itself
                    if TASK_ALLOW_MULTIPLE_INSTANCES:
                        main_logger.warn("wait for 5 seconds...")
                        time.sleep(5)
                    else:
                        main_logger.warn("Multiple tasks execution is disabled, exiting...")
                        return False
            else:
                main_logger.warn("Task w/ pid " + lock_pid + " has completed, resume...")
                return True
    else:
        return True

# Creates lock file and write process PID in to it
def create_lock_file():
    with open(TASK_LOCK_FILE, "w") as lock_file:
        lock_file.write(TASK_PID)

# Removes lock file if it exists else does nothing
def remove_lock_file():
    if os.path.isfile(TASK_LOCK_FILE):
        os.remove(TASK_LOCK_FILE)

# Initializes zReport task
def init():
    if check_lock_file():
        create_lock_file()
        main_logger.info("Started")
    else:
        sys.exit(0)

# Finish zReport task
def finish():
    remove_lock_file()
    main_logger.info("Completed")

init()

wait_for_reports()

finish()