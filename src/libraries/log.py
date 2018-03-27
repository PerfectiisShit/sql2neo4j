# -*- coding: utf-8 -*-

import logging
import os
from datetime import datetime

from libraries.config import config


class Logger(object):
    def __init__(self, file_path, level):
        self.logger = logging.getLogger("sql2neo4j")
        self.now = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        self.formatter = logging.Formatter('[%(levelname)s] [%(log_type)s]: %(message)s')
        self.handler = logging.FileHandler(file_path + '-' + self.now)
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)
        if level.lower() == "debug":
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

    def _log(self, msg, log_type, traceback, log_level="info"):
        extra = {'log_type': log_type}
        logger = logging.LoggerAdapter(self.logger, extra)
        try:
            getattr(logger, log_level)(unicode(msg))
        except NameError:
            getattr(logger, log_level)(msg)
        except AttributeError:
            logger.error("Unknown log level %s" % log_level, {'log_type': "Logging"})

        if traceback:
            exc_msg = "Error details: "
            try:
                logger.exception(unicode(exc_msg))
            except NameError:
                logger.exception(exc_msg)

    def info(self, msg, log_type="SQL2Neo4j", traceback=False):
        self._log(msg, log_type, traceback, "info")

    def debug(self, msg, log_type="SQL2Neo4j", traceback=False):
        self._log(msg, log_type, traceback, "debug")

    def error(self, msg, log_type="SQL2Neo4j", traceback=False):
        self._log(msg, log_type, traceback, "error")

    def warn(self, msg, log_type="SQL2Neo4j", traceback=False):
        self._log(msg, log_type, traceback, "warning")


__logger = None


def get_logger():
    global __logger
    if __logger is None:
        file_path = config.get('LOG_PATH', '')
        level = config.get('LOG_LEVEL', 'INFO')
        if file_path != '' and os.path.exists(os.path.dirname(file_path)):
            __logger = Logger(file_path, level)
        else:
            raise OSError("Log folder %s does not exist!" % os.path.dirname(file_path))
    return __logger
