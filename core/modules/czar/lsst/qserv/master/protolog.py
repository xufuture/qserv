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

from lsst.qserv.master import initLog_iface
from lsst.qserv.master import log_iface
import inspect

ROOT_LOGGER_NAME = "root"

# logging levels
TRACE = 0
DEBUG = 1
INFO = 2
WARN = 3
ERROR = 4
FATAL = 5

def initLog():
    initLog_iface()

def trace(msg):
    log(ROOT_LOGGER_NAME, TRACE, msg)

def debug(msg):
    log(ROOT_LOGGER_NAME, DEBUG, msg)

def info(msg):
    log(ROOT_LOGGER_NAME, INFO, msg)

def warn(msg):
    log(ROOT_LOGGER_NAME, WARN, msg)

def error(msg):
    log(ROOT_LOGGER_NAME, ERROR, msg)

def fatal(msg):
    log(ROOT_LOGGER_NAME, FATAL, msg)

def log(loggerName, level, msg):
    f = inspect.currentframe().f_back
    funcname = inspect.stack()[1][3]
    log_iface(f.f_code.co_filename, funcname, f.f_lineno, loggerName, level, msg)
    
