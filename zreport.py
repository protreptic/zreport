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
import argparse

# Paths configuration
REPORT_DIRECTORY = "data"
REPORT_CHECK_INTERVAL = 15
REPORT_NAME_TEMPLATE = "ZReport*.xml"

# Archiving configuration
ARCHIVE_DIRECTORY = "archive"
ARCHIVE_STORE_LIMIT = 31556926  # ~1 year

# Task configuration
TASK_PID = str(os.getpid())
TASK_LOCK_FILE = "zreport.lock"
TASK_TOTAL_EXECUTION_TIME = 0
MAX_EXECUTION_TIME = 10  # In seconds
ALLOW_MULTIPLE = False

# Logging configuration
LOG_DIRECTORY = "log"
LOG_FILE = LOG_DIRECTORY+"/zreport.log"
LOG_MAX_SIZE = 1024 * 1024 * 2  # 2Mb
LOG_BACKUP_COUNT = 5

# Test configuration
TEST = True

# Production FTP configuration
FTP_SERVER = "localhost"
FTP_SERVER_PATH = "./IdeaProjects/zreport/ftpdest1"
FTP_USERNAME = "peterbukhal"
FTP_PASSWORD = "123"

# Test FTP configuration
TEST_FTP_SERVER = "localhost"
TEST_FTP_SERVER_PATH = "./IdeaProjects/zreport/ftpdest1"
TEST_FTP_USERNAME = "peterbukhal"
TEST_FTP_PASSWORD = "123"

# Initialize logger
def init_logger():
    # Create log folder if it not exists
    if not os.path.isdir(LOG_DIRECTORY):
        os.mkdir(LOG_DIRECTORY)

    logger = logging.getLogger("zReport")
    logger.setLevel("INFO")
    default_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s {pid=%(process)d}: %(message)s")

    file_handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=LOG_MAX_SIZE, backupCount=LOG_BACKUP_COUNT)
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

    if diff > MAX_EXECUTION_TIME:
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
    shutil.copy(report, archived_report)
    main_logger.info("Report " + os.path.basename(report) + " was successfully archived")

def remove_old_reports():
    if not os.path.isdir(ARCHIVE_DIRECTORY):
        os.mkdir(ARCHIVE_DIRECTORY)

    for report in glob.glob(ARCHIVE_DIRECTORY + "/" + REPORT_NAME_TEMPLATE):
        diff = int(time.time() - os.path.getctime(TASK_LOCK_FILE))

        if diff >= ARCHIVE_STORE_LIMIT:
            os.remove(report)
            main_logger.info("Report " + os.path.basename(report) + " removed from archive (store time exceeded)")

#
def wait_for_reports():
    main_logger.info("Waiting for reports... (check it every ~" + str(REPORT_CHECK_INTERVAL) + " seconds)")
    while True:
        if time_exceeded():
            main_logger.info("Task has exceeded max execution time and will be stopped normally")

            return
        else:
            for report in glob.glob(REPORT_DIRECTORY + "/" + REPORT_NAME_TEMPLATE):
                send_report_to_server(report)

            time.sleep(REPORT_CHECK_INTERVAL)

# Tries to terminate given process by it`s PID
# if fails returns False
# else if process with PID was terminated returns True
def terminate_task(pid):
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

                if diff > MAX_EXECUTION_TIME:
                    main_logger.warn("Task w/ pid " + lock_pid + " has exceeded max exe time and will be terminated")

                    if terminate_task(lock_pid):
                        main_logger.warn("Task w/ pid " + lock_pid + " was successfully terminated")
                    else:
                        main_logger.warn("Task process w/ pid " + lock_pid + " was not found, skip task termination")

                    remove_lock_file()

                    return True
                else:
                    # If another task is running and TASK_ALLOW_MULTIPLE_INSTANCES enabled
                    # wait for another task to complete else stop itself
                    if ALLOW_MULTIPLE:
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

# Creates lock file and write process PID into it
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
    remove_old_reports()
    remove_lock_file()
    main_logger.info("Completed")

configuration = {
    # Paths configuration
    "--report-directory": REPORT_DIRECTORY,
    "--report-check-interval": REPORT_CHECK_INTERVAL,
    "--report-name-template": REPORT_NAME_TEMPLATE,

    # Archiving configuration
    "--archive-directory": ARCHIVE_DIRECTORY,
    "--archive-store-limit": ARCHIVE_STORE_LIMIT,

    # Task configuration
    "--task-max-execution-time": 10,
    "--task-allow-multiple-tasks": 0,

    # Logging configuration
    "--log-directory": "log",
    "--log-file": "zreport.log",
    "--log-file-max-size": 1024 * 1024 * 2,
    "--log-file-backup-count": 5,

    # Test configuration
    "--test": True,

    # Production FTP connection configuration
    "--ftp-server": "localhost",
    "--ftp-server_path": "./IdeaProjects/zreport/ftpdest1",
    "--ftp-username": "peterbukhal",
    "--ftp-password": "123",

    # Test FTP configuration
    "--test-ftp-server": "localhost",
    "--test-ftp-server_path": "./IdeaProjects/zreport/ftpdest1",
    "--test-ftp-username": "peterbukhal",
    "--test-ftp-password": "123"
}

parser = argparse.ArgumentParser("")
parser.add_argument("--report-directory",
                    type=str, metavar="<path>", default=REPORT_DIRECTORY,
                    help="A path to a directory that contains Z-reports")
parser.add_argument("--report-check-interval",
                    type=int, metavar="<interval>", default=REPORT_CHECK_INTERVAL,
                    help="An interval in seconds that uses for ")
parser.add_argument("--report-name-template",
                    type=str, metavar="<template>", default=REPORT_NAME_TEMPLATE,
                    help="")
parser.add_argument("--archive-directory",
                    type=str, metavar="<path>", default=ARCHIVE_DIRECTORY,
                    help="A path to a directory that contains archived Z-reports")
parser.add_argument("--archive-store-limit",
                    type=str, metavar="<seconds>", default=ARCHIVE_STORE_LIMIT,
                    help="Defines period of time during which reports will be stored in archive")
parser.add_argument("--max-execution-time",
                    type=int, metavar="<seconds>", default=MAX_EXECUTION_TIME,
                    help="")
parser.add_argument("--allow-multiple",
                    type=bool, metavar="", default=ALLOW_MULTIPLE,
                    help="If present allows to run multiple copies of the task, with will be executed")
parser.add_argument("--log-directory",
                    type=str, metavar="<path>", default=LOG_DIRECTORY,
                    help="A path to a directory that contains logs")
parser.add_argument("--log-max-size",
                    type=int, metavar="<bytes>", default=LOG_MAX_SIZE,
                    help="Log file max size in bytes")
parser.add_argument("--log-backup-count",
                    type=int, metavar="<number>", default=LOG_BACKUP_COUNT,
                    help="Defines how many ")
parser.add_argument("--test",
                    type=bool, metavar="", default=TEST,
                    help="If present sends Z-reports to test ftp server instead of production server")
parser.add_argument("--ftp-server",
                    type=str, metavar="<ip-address or domain>", default=FTP_SERVER,
                    help="Production FTP server ip-address or domain name")
parser.add_argument("--ftp-server-path",
                    type=str, metavar="<path>", default=FTP_SERVER_PATH,
                    help="A path to a target directory on the production server")
parser.add_argument("--ftp-username",
                    type=str, metavar="<username>", default=FTP_USERNAME,
                    help="User name on production FTP server")
parser.add_argument("--ftp-password",
                    type=str, metavar="<password>", default=FTP_PASSWORD,
                    help="Password for user on production FTP server")
parser.add_argument("--test-ftp-server",
                    type=str, metavar="<ip-address or domain>", default=TEST_FTP_SERVER,
                    help="Test FTP server ip-address or domain name")
parser.add_argument("--test-ftp-server-path",
                    type=str, metavar="<path>", default=TEST_FTP_SERVER_PATH,
                    help="A path to a target directory on the test server")
parser.add_argument("--test-ftp-username",
                    type=str, metavar="<username>", default=TEST_FTP_USERNAME,
                    help="User name on test FTP server")
parser.add_argument("--test-ftp-password",
                    type=str, metavar="<password>", default=TEST_FTP_PASSWORD,
                    help="Password for user on test FTP server")

args = parser.parse_args()

class ZReport:

    def __init__(self):
        pass

    def run(self):
        init()
        wait_for_reports()
        finish()

if __name__ == "__main__":
    ZReport().run()