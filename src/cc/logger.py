import logging
import sys
import os
from enum import Enum

CcLoggingLevel = "CC_LOGGING_LEVEL"

CcLoggingLevels = {
    "NOTSET": logging.NOTSET,
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARN": logging.WARN,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def initLogger():
    cc_log_level = os.environ.get(CcLoggingLevel, "INFO")
    # logging.basicConfig(stream=sys.stdout, level=CcLoggingLevels[level], format="%(levelname)s | %(asctime)s | %(message)s")
    logging.basicConfig(stream=sys.stdout, format="%(levelname)s | %(asctime)s | %(message)s", level=CcLoggingLevels[cc_log_level])
    # logging.basicConfig(format="%(levelname)s | %(asctime)s | %(message)s")
    # logging.basicConfig(format="%(manifest)s - %(message)s")
    # file_handler = logging.FileHandler(filename="tmp.log")
    # stdout_handler = logging.StreamHandler(stream=sys.stdout)
    # handlers = [file_handler, stdout_handler]
    # # logging.basicConfig(level=logging.DEBUG, format=f"{manifest}: %(levelname)s | %(asctime)s | %(message)s", handlers=handlers)
    # logging.basicConfig(level=logging.DEBUG, format="%(levelname)s | %(asctime)s | %(message)s", handlers=handlers)
    # logging.info("ASDFASDFASDF")

    # rec_factory = logging.getLogRecordFactory()

    # def record_factory(*args, **kwargs):
    #     record = rec_factory(*args, **kwargs)
    #     record.custom_attribute = "manifest"
    #     return record

    # logging.setLogRecordFactory(record_factory)
