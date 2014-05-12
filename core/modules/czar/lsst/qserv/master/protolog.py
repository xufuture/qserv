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

from lsst.qserv.master import initLog_iface, getDefaultLoggerName_iface,\
                              pushContext_iface, popContext_iface, MDC_iface,\
                              MDCRemove_iface, getLevel_iface, setLevel_iface,\
                              isEnabledFor_iface, log_iface
import logging
import inspect
from os import path

# logging levels (these conform to standard log4cxx levels)
TRACE = 5000
DEBUG = 10000
INFO = 20000
WARN = 30000
ERROR = 40000
FATAL = 50000

def initLog(filename):
    initLog_iface(filename)

def getDefaultLoggerName():
    name = getDefaultLoggerName_iface()
    return name

def pushContext(c):
    pushContext_iface(c)

def popContext():
    popContext_iface()
    
def MDC(key, value):
    MDC_iface(str(key), str(value))

def MDCRemove(key):
    MDCRemove_iface(str(key))

def getLevel(loggerName):
    return getLevel_iface(loggerName)

def setLevel(loggerName, level):
    setLevel_iface(loggerName, level)

def isEnabledFor(loggerName, level):
    return isEnabledFor_iface(loggerName, level)
    
def log(loggerName, level, fmt, *args, **kwargs):
    if 'depth' in kwargs:
        depth = kwargs['depth']
    else:
        depth = 1
    if isEnabledFor(loggerName, level):
        frame = getFrame(depth)
        log_iface(loggerName, level, path.split(frame.f_code.co_filename)[1],
                      getFuncName(depth), frame.f_lineno, fmt % args)

def getFrame(depth):
    frame = inspect.currentframe().f_back
    for i in range(depth):
        frame = frame.f_back
    return frame

def getFuncName(depth):
    return inspect.stack()[depth+1][3]

class ProtoLogContext:

    def __init__(self, name=None, level=None):
        self.name = name
        self.level = level

    def __enter__(self):
        self.open()
        return self

    def __exit__(self ,type, value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def open(self):
        if self.name is not None:
            pushContext(self.name)
        if self.level is not None:
            setLevel("", self.level)

    def close(self):
        if self.name is not None:
            popContext()
            self.name = None

    def getLevel(self):
        return getLevel("")

    def setLevel(self, level):
        setLevel("", level)

    def isEnabledFor(self, level):
        return isEnabledFor("", level)

    def trace(self, fmt, *args):
        log("", TRACE, fmt, *args, depth=2)
    
    def debug(self, fmt, *args):
        log("", DEBUG, fmt, *args, depth=2)
    
    def info(self, fmt, *args):
        log("", INFO, fmt, *args, depth=2)
    
    def warn(self, fmt, *args):
        log("", WARN, fmt, *args, depth=2)
    
    def error(self, fmt, *args):
        log("", ERROR, fmt, *args, depth=2)
    
    def fatal(self, fmt, *args):
        log("", FATAL, fmt, *args, depth=2)

class ProtoLogHandler(logging.Handler):
    def __init__(self, name=None, level=None):
        self.context = ProtoLogContext(name=name, level=level)
        self.context.open()
        logging.Handler.__init__(self)

    def __del__(self):
        self.close()
        logging.Handler.__del__(self)

    def close(self):
        if self.context is not None:
            self.context.close()
            self.context = None
        logging.Handler.close(self)

    def handle(self, record):
        if self.context.isEnabledFor(self.translateLevel(record.levelno)):
            logging.Handler.handle(self, record)

    def emit(self, record):
        log_iface("", self.translateLevel(record.levelno), record.filename,
                  record.funcName, record.lineno, record.msg % record.args)

    def translateLevel(self, levelno):
        """
        Translates from standard python logging module levels
        to standard log4cxx levels.
        """
        return levelno*1000
