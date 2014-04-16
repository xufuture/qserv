#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

from lsst.qserv.master import initLog_iface, pushContext_iface,\
                              popContext_iface, MDC_iface, log_iface
import logging
import inspect

# logging levels
TRACE = 0
DEBUG = 1
INFO = 2
WARN = 3
ERROR = 4
FATAL = 5

def initLog():
    initLog_iface()

def pushContext(c):
    pushContext_iface(c)

def popContext():
    popContext_iface()

def MDC(key, value):
    MDC_iface(str(key), str(value))

def trace(msg):
    log("", TRACE, msg)

def debug(msg):
    log("", DEBUG, msg)

def info(msg):
    log("", INFO, msg)

def warn(msg):
    log("", WARN, msg)

def error(msg):
    log("", ERROR, msg)

def fatal(msg):
    log("", FATAL, msg)

def log(loggerName, level, msg):
    f = inspect.currentframe().f_back
    funcname = inspect.stack()[1][3]
    log_iface(loggerName, level, f.f_code.co_filename, funcname, f.f_lineno,
              msg)

class ProtoLogHandler(logging.Handler):
    def emit(self, record):
        log_iface(record.name, translateLevel(record.levelno), record.filename,
                  record.funcName, record.lineno, record.msg % record.args)

    def translateLevel(levelno):
        return level/10
