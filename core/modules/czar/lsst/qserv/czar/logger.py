#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008-2014 LSST Corporation.
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

# logger.py : A module with a logging interface that utilizes SWIG
# enabled Logger class.

# Package imports
from lsst.qserv.czar import logger_threshold
from lsst.qserv.czar import logger

# Import new logging module
import lsst.log as newlog

# Toggles new, log4cxx-based logging on/off for Qserv's Python-layer
NEWLOG = True

def threshold_dbg():
    if NEWLOG:
        newlog.setLevel("", newlog.DEBUG)
    else:
        logger_threshold(0)

def threshold_inf():
    if NEWLOG:
        newlog.setLevel("", newlog.INFO)
    else:
        logger_threshold(1)

def threshold_wrn():
    if NEWLOG:
        newlog.setLevel("", newlog.WARN)
    else:
        logger_threshold(2)

def threshold_err():
    if NEWLOG:
        newlog.setLevel("", newlog.ERROR)
    else:
        logger_threshold(3)

def newlog_msg(level, *args):
    newlog.log("", level, ' '.join(map(str, args)), depth=4)

def log_msg(level, *args):
    logger(level, ' '.join(map(str, args)))

def dbg(*args):
    if NEWLOG:
        newlog_msg(newlog.DEBUG, args)
    else:
        log_msg(0, args)

def inf(*args):
    if NEWLOG:
        newlog_msg(newlog.INFO, args)
    else:
        log_msg(1, args)

def wrn(*args):
    if NEWLOG:
        newlog_msg(newlog.WARN, args)
    else:
        log_msg(2, args)

def err(*args):
    if NEWLOG:
        newlog_msg(newlog.ERROR, args)
    else:
        log_msg(3, args)

