#!/usr/bin/env python

#
# LSST Data Management System
# Copyright 2013 LSST Corporation.
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

"""
Python-layer of LSST DM logging module built on log4cxx.

@author Bill Chickering
Contact: chickering@cs.stanford.edu
"""

from lsst.qserv.master import configure_iface, getDefaultLoggerName_iface,\
                              pushContext_iface, popContext_iface, MDC_iface,\
                              MDCRemove_iface, getLevel_iface, setLevel_iface,\
                              isEnabledFor_iface, forcedLog_iface
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

def configure(*args):
    """
    Configure logging module.

    @param args  Optionally include file path to either a log4cxx XML or
                 log4j Java properties configuration file.
    """
    if len(args) > 0:
        configure_iface(args[0])
    else:
        configure_iface()

def getDefaultLoggerName():
    """
    Retrieve the default logger's name.
    @return String containing default logger name.
    """
    name = getDefaultLoggerName_iface()
    return name

def pushContext(name):
    """
    Push a name onto the logging context.

    @param name  String containing name to be appended to the logging
                 context, which consists of a sequence of names separated
                 by '.'.
    """
    pushContext_iface(name)

def popContext():
    """
    Pop off the last name added to the logging context.
    """
    popContext_iface()
    
def MDC(key, value):
    """
    Insert a KEY/VALUE pair into the MDC.

    Places a KEY/VALUE pair in the Mapped Diagnostic Context (MDC) for the
    current thread. The VALUE may then be included in log messages by using
    the following the `X` conversion character within a pattern layout as
    `%X{KEY}`.

    @param key    Unique key.
    @param value  String value.
    """
    MDC_iface(str(key), str(value))

def MDCRemove(key):
    """
    Remove the value associated with KEY within the MDC.

    @param key  Key identifying value to remove.
    """
    MDCRemove_iface(str(key))

def setLevel(loggername, level):
    """
    Set the logging threshold for the logger named LOGGERNAME to LEVEL.

    @param loggername  Name of logger with threshold to adjust.
    @param level       New logging threshold.
    """
    setLevel_iface(loggername, level)

def getLevel(loggername):
    """
    Retrieve the logging threshold for the logger with name LOGGERNAME.
    @return Int indicating the logging threshold.

    @param loggername  Name of logger with threshold to return.
    """
    return getLevel_iface(loggername)

def isEnabledFor(loggername, level):
    """
    Return whether the logging threshold of the logger with name LOGGERNAME
    is less than or equal to LEVEL.
    @return Bool indicating whether or not logger is enabled.
    
    @param loggername  Name of logger being queried.
    @param level       Logging threshold to check.
    """
    return isEnabledFor_iface(loggername, level)
    
def _getFrame(depth):
    """
    Private function to retrieve the frame from the stack at a particular
    depth.
    @return Frame object.

    @param depth  Positive int indicating stack depth.
    """
    frame = inspect.currentframe().f_back
    for i in range(depth):
        frame = frame.f_back
    return frame

def _getFuncName(depth):
    """
    Private function to retrieve the function name associated with the stack
    frame at a particular depth.
    @return String containing function name.

    @param depth  Positive int indicating stack depth.
    """
    return inspect.stack()[depth+1][3]

def log(loggername, level, fmt, *args, **kwargs):
    """
    Log a message.

    @param loggername  Name of logger to use.
    @param level       Logging level of message.
    @param fmt         Printf-style format string.
    @param *args       List of variables to apply to format string.
    @param **kwargs    Key/value pairs used by logging module.
    """
    if 'depth' in kwargs:
        depth = kwargs['depth']
    else:
        depth = 1
    if isEnabledFor(loggername, level):
        frame = _getFrame(depth)
        forcedLog_iface(loggername, level,
                        path.split(frame.f_code.co_filename)[1],
                        _getFuncName(depth), frame.f_lineno, fmt % args)

def trace(fmt, *args):
    """
    Log a trace-level message to the default logger.

    @param fmt  Printf-style format string.
    @*args      List of variables to apply to format string.
    """
    log("", TRACE, fmt, *args, depth=2)

def debug(fmt, *args):
    """
    Log a debug-level message to the default logger.

    @param fmt  Printf-style format string.
    @*args      List of variables to apply to format string.
    """
    log("", DEBUG, fmt, *args, depth=2)

def info(fmt, *args):
    """
    Log a info-level message to the default logger.

    @param fmt  Printf-style format string.
    @*args      List of variables to apply to format string.
    """
    log("", INFO, fmt, *args, depth=2)

def warn(fmt, *args):
    """
    Log a warn-level message to the default logger.

    @param fmt  Printf-style format string.
    @*args      List of variables to apply to format string.
    """
    log("", WARN, fmt, *args, depth=2)

def error(fmt, *args):
    """
    Log a error-level message to the default logger.

    @param fmt  Printf-style format string.
    @*args      List of variables to apply to format string.
    """
    log("", ERROR, fmt, *args, depth=2)

def fatal(fmt, *args):
    """
    Log a fatal-level message to the default logger.

    @param fmt  Printf-style format string.
    @*args      List of variables to apply to format string.
    """
    log("", FATAL, fmt, *args, depth=2)

class ProtoLogContext:
    """
    Proxy for handling the default logging context.
    """

    def __init__(self, name=None, level=None):
        """
        Instigate a new logging context.

        @param name   Name to push onto the logging context. If None (default)
                      this object will correspond to the preexisting logging
                      default logging context.
        @param level  Logging threshold to assign to the default logger
                      associated with this context. If None (default) the
                      logging threshold is determined as per normal (e.g.
                      configuration or level inheritance).
        """
        self.name = name
        self.level = level

    def __enter__(self):
        """
        Invoked upon entering the runtime context.
        """
        self.open()
        return self

    def __exit__(self, type, value, traceback):
        """
        Invoked upon exiting the runtime context.

        @param type       Type of exception that caused the context to be
                          exited. Equals None if context was exited without
                          exception.
        @param value      Value of exception that caused the context to be
                          exited. Equals None if context was exited without
                          exception.
        @param traceback  Traceback associated with exception that caused the
                          context to be exited. Equals None if context was
                          exited without exception.
        """
        self.close()

    def __del__(self):
        """
        Invoked when context object is about to be destroyed.
        """
        self.close()

    def open(self):
        """
        If a name was provided, pushes it onto the default logging context.
        If a level was provided, assigns it as the logging threshold of the
        default logger.
        """
        if self.name is not None:
            pushContext(self.name)
        if self.level is not None:
            setLevel("", self.level)

    def close(self):
        """
        If a name was provided, pops the last name pushed onto the logging
        context.
        """
        if self.name is not None:
            popContext()
            self.name = None

    def setLevel(self, level):
        """
        Set the logging threshold for the default logger.

        @param level  New logging threshold.
        """
        setLevel("", level)

    def getLevel(self):
        """
        Retrieve the logging threshold for the default logger.
        @return Int indicating the logging threshold.
        """
        return getLevel("")

    def isEnabledFor(self, level):
        """
        Return whether the logging threshold of the deafult logger is less
        than or equal to LEVEL.
        @return Bool indicating whether or not logger is enabled.

        @param level  Logging threshold to check.
        """
        return isEnabledFor("", level)

class ProtoLogHandler(logging.Handler):
    """
    A logging handler for use with Python's standard logging module that
    directs messages to the LSST DM logging module. This class acts in part
    as a wrapper for the ProtoLogContext class.
    """

    def __init__(self, name=None, level=None):
        """
        Create and initialize handler.

        @param name   Context name to pass to ProtoLogContext.
        @param level  Logging threshold to pass to ProtoLogContext.
        """
        self.context = ProtoLogContext(name=name, level=level)
        self.context.open()
        logging.Handler.__init__(self)

    def __del__(self):
        """
        Invoked when handler object is about to be destroyed.
        """
        self.close()

    def close(self):
        """
        Overridden method that invokes the close() method of the wrapped
        ProtoLogContext instance as well as that of the handler's superclass.
        """
        if self.context is not None:
            self.context.close()
            self.context = None
        logging.Handler.close(self)

    def handle(self, record):
        """
        Overridden method that filters logging messages by level based on the
        threshold of the wrapped ProtoLogContext. If threshold is met, the
        handler's superclass's handle() method is invoked.

        @param record  Logging record object.
        """
        if self.context.isEnabledFor(self._translateLevel(record.levelno)):
            logging.Handler.handle(self, record)

    def emit(self, record):
        """
        Overridden method that utilizes the SWIG'd logging API to pass log
        record to C++ layer.

        @param record  Logging record object.
        """
        forcedLog_iface("", self._translateLevel(record.levelno), record.filename,
                  record.funcName, record.lineno, record.msg % record.args)

    def _translateLevel(self, levelno):
        """
        Private method that translates from standard python logging module
        levels to standard log4cxx levels.
        @return Int corresponding to log4cxx logging level.

        @param levelno  Int corresponding to Python logging level.
        """
        return levelno*1000
