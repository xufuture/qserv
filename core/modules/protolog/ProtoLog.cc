/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
// See ProtoLog.h

#include "ProtoLog.h"
#include <log4cxx/consoleappender.h>
#include <log4cxx/simplelayout.h>
#include <log4cxx/logmanager.h>
#include <log4cxx/xml/domconfigurator.h>

using namespace lsst::qserv;

// ProtoLog class

std::stack<std::string> ProtoLog::context;
std::string ProtoLog::defaultLogger;

void ProtoLog::initLog(std::string const& filename) {
    // Load XML configuration file using DOMConfigurator
    log4cxx::xml::DOMConfigurator::configure(filename);
    // Clear context
    while (!context.empty()) context.pop();
}

std::string ProtoLog::getDefaultLoggerName() {
    return defaultLogger;
}

log4cxx::LoggerPtr ProtoLog::getLogger(std::string const& loggername) {
    log4cxx::LoggerPtr logger;
    if (loggername.empty())
        logger = log4cxx::Logger::getLogger(defaultLogger);
    else
        logger = log4cxx::Logger::getLogger(loggername);
    return logger;
}

void ProtoLog::pushContext(std::string const& c) {
    context.push(c);
    // construct new default logger name
    std::stringstream ss;
    if (defaultLogger.empty())
        ss << c;
    else
        ss << defaultLogger << "." << c;
    defaultLogger = ss.str();
}

void ProtoLog::popContext() {
    context.pop();
    // construct new default logger name
    std::string::size_type pos = defaultLogger.find_last_of('.');
    defaultLogger = defaultLogger.substr(0, pos);
}

void ProtoLog::MDC(std::string const& key, std::string const& value) {
    log4cxx::MDC::put(key, value);
}

void ProtoLog::MDCRemove(std::string const& key) {
    log4cxx::MDC::remove(key);
}

void ProtoLog::setLevel(std::string const& loggername, int level) {
    getLogger(loggername)->setLevel(log4cxx::Level::toLevel(level));
}

int ProtoLog::getLevel(std::string const& loggername) {
    log4cxx::LevelPtr levelPtr = getLogger(loggername)->getLevel();
    int level = -1; // TODO: Is this the right thing to do?
    if (levelPtr != NULL)
        level = levelPtr->toInt();
    return level;
}

bool ProtoLog::isEnabledFor(std::string const& loggername, int level) {
    log4cxx::LoggerPtr logger = getLogger(loggername);
    if (logger->isEnabledFor(log4cxx::Level::toLevel(level)))
        return true;
    else
        return false;
}

void ProtoLog::forcedLog(log4cxx::LoggerPtr logger,
                         const log4cxx::LevelPtr &level,
                         std::string const& filename,
                         std::string const& funcname,
                         unsigned int lineno,
                         std::string const& msg) {
    logger->forcedLog(level, msg.c_str(),
                      log4cxx::spi::LocationInfo(filename.c_str(),
                                                 funcname.c_str(),
                                                 lineno));
}

// ProtoLogContext class

ProtoLogContext::ProtoLogContext(std::string const& name) {
    _name = name;
    ProtoLog::pushContext(name);
}

ProtoLogContext::~ProtoLogContext() {
    if (!_name.empty())
        ProtoLog::popContext();
}

// ProtoLogFormatter class

ProtoLogFormatter::ProtoLogFormatter(std::string const& loggername, int level,
                                     std::string const& filename,
                                     std::string const& funcname,
                                     unsigned int lineno, const char* fmt) :
                                     _logger(ProtoLog::getLogger(loggername)),
                                     _level(level),
                                     _filename(filename),
                                     _funcname(funcname),
                                     _lineno(lineno),
                                     _fmter(fmt) {
}

ProtoLogFormatter::ProtoLogFormatter(log4cxx::LoggerPtr logger, int level,
                                     std::string const& filename,
                                     std::string const& funcname,
                                     unsigned int lineno, const char* fmt) :
                                     _logger(logger),
                                     _level(level),
                                     _filename(filename),
                                     _funcname(funcname),
                                     _lineno(lineno),
                                     _fmter(fmt) {
}

ProtoLogFormatter::~ProtoLogFormatter() {
    log4cxx::LevelPtr levelPtr = log4cxx::Level::toLevel(_level);
    if (_logger->isEnabledFor(levelPtr))
        ProtoLog::forcedLog(_logger, levelPtr, _filename, _funcname, _lineno,
                            _fmter.str());
}

