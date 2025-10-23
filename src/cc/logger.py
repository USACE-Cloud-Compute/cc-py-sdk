import logging
import sys
import os

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
    logging.basicConfig(
        stream=sys.stdout,
        format="%(levelname)s | %(asctime)s | %(message)s",
        level=CcLoggingLevels[cc_log_level],
    )
    # file_handler = logging.FileHandler(filename="tmp.log")
    # stdout_handler = logging.StreamHandler(stream=sys.stdout)
    # handlers = [file_handler, stdout_handler]

    # rec_factory = logging.getLogRecordFactory()

    # def record_factory(*args, **kwargs):
    #     record = rec_factory(*args, **kwargs)
    #     record.custom_attribute = "manifest"
    #     return record

    # logging.setLogRecordFactory(record_factory)
