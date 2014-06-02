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
#include <log4cxx/basicconfigurator.h>
#include <log4cxx/xml/domconfigurator.h>
#include <log4cxx/propertyconfigurator.h>
#include <stdio.h>

// Max message length for varargs/printf style logging
#define MAX_LOG_MSG_LEN 1024

using namespace lsst::qserv;

// ProtoLogFormatter class

ProtoLogFormatter::ProtoLogFormatter() : _enabled(false) {}
    
ProtoLogFormatter::~ProtoLogFormatter() {
    if (_enabled) delete _fmter;
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

// ProtoLog class

log4cxx::LoggerPtr ProtoLog::defaultLogger = log4cxx::Logger::getRootLogger();
std::stack<std::string> ProtoLog::context;
std::string ProtoLog::defaultLoggerName;

void ProtoLog::initLog() {
    // Clear context
    while (!context.empty()) context.pop();
    // Default logger initially set to root logger
    defaultLogger = log4cxx::Logger::getRootLogger();
}

void ProtoLog::configure() {
    // Is there a valid default configuration file from the environment?
    // We check for this by sending a message to root logger. If default
    // configuration exists, the root logger will have at least one
    // appender afterward. Otherwise, we will use log4cxx's basic
    // configuration.
    log4cxx::LoggerPtr rootLogger = log4cxx::Logger::getRootLogger();
    LOG4CXX_INFO(rootLogger, "Initializing Logging System");
    if (rootLogger->getAllAppenders().size() == 0)
        log4cxx::BasicConfigurator::configure();
    initLog();
}

std::string getFileExtension(std::string const& filename) {
    size_t dotpos = filename.find_last_of(".");
    if (dotpos == std::string::npos) return "";
    return filename.substr(dotpos, filename.size() - dotpos);
}

void ProtoLog::configure(std::string const& filename) {
    log4cxx::BasicConfigurator::resetConfiguration();
    if (getFileExtension(filename).compare(".xml") == 0)
        // Load XML configuration file using DOMConfigurator
        log4cxx::xml::DOMConfigurator::configure(filename);
    else
        // Otherwise, assume Java properties file
        log4cxx::PropertyConfigurator::configure(filename);
    initLog();
}

std::string ProtoLog::getDefaultLoggerName() {
    return defaultLoggerName;
}

// Used to simplify macros
log4cxx::LoggerPtr ProtoLog::getLogger(log4cxx::LoggerPtr logger) {
    return logger;
}

log4cxx::LoggerPtr ProtoLog::getLogger(std::string const& loggername) {
    if (loggername.empty())
        return defaultLogger;
    else
        return log4cxx::Logger::getLogger(loggername);
}

void ProtoLog::pushContext(std::string const& name) {
    context.push(name);
    // Construct new default logger name
    std::stringstream ss;
    if (defaultLoggerName.empty())
        ss << name;
    else
        ss << defaultLoggerName << "." << name;
    defaultLoggerName = ss.str();
    // Update defaultLogger
    defaultLogger = log4cxx::Logger::getLogger(defaultLoggerName);
}

void ProtoLog::popContext() {
    context.pop();
    // construct new default logger name
    std::string::size_type pos = defaultLoggerName.find_last_of('.');
    defaultLoggerName = defaultLoggerName.substr(0, pos);
    // Update defaultLogger
    defaultLogger = log4cxx::Logger::getLogger(defaultLoggerName);
}

void ProtoLog::MDC(std::string const& key, std::string const& value) {
    log4cxx::MDC::put(key, value);
}

void ProtoLog::MDCRemove(std::string const& key) {
    log4cxx::MDC::remove(key);
}

void ProtoLog::setLevel(log4cxx::LoggerPtr logger, int level) {
    logger->setLevel(log4cxx::Level::toLevel(level));
}

void ProtoLog::setLevel(std::string const& loggername, int level) {
    setLevel(getLogger(loggername), level);
}

int ProtoLog::getLevel(log4cxx::LoggerPtr logger) {
    log4cxx::LevelPtr level = logger->getLevel();
    int levelno = -1;
    if (level != NULL)
        levelno = level->toInt();
    return levelno;
}

int ProtoLog::getLevel(std::string const& loggername) {
    return getLevel(getLogger(loggername));
}

bool ProtoLog::isEnabledFor(log4cxx::LoggerPtr logger, int level) {
    if (logger->isEnabledFor(log4cxx::Level::toLevel(level)))
        return true;
    else
        return false;
}

bool ProtoLog::isEnabledFor(std::string const& loggername, int level) {
    return isEnabledFor(getLogger(loggername), level);
}

// varargs/printf style logging

void ProtoLog::vlog(log4cxx::LoggerPtr logger, log4cxx::LevelPtr level,
                    std::string const& filename, std::string const& funcname,
                    unsigned int lineno, std::string const& fmt, va_list args) {
    char msg[MAX_LOG_MSG_LEN];
    vsnprintf(msg, MAX_LOG_MSG_LEN, fmt.c_str(), args);
    logger->forcedLog(level, msg, log4cxx::spi::LocationInfo(filename.c_str(),
                                                             funcname.c_str(),
                                                             lineno));
}

void ProtoLog::log(std::string const& loggername, log4cxx::LevelPtr level,
                   std::string const& filename, std::string const& funcname,
                   unsigned int lineno, std::string const& fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vlog(getLogger(loggername), level, filename, funcname, lineno, fmt, args);
}

void ProtoLog::log(log4cxx::LoggerPtr logger, log4cxx::LevelPtr level,
                   std::string const& filename, std::string const& funcname,
                   unsigned int lineno, std::string const& fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vlog(logger, level, filename, funcname, lineno, fmt, args);
}

