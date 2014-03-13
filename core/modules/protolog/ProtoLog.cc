/*
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
#include <log4cxx/logger.h>
#include <log4cxx/consoleappender.h>
#include <log4cxx/simplelayout.h>
#include <log4cxx/logmanager.h>
#include <log4cxx/xml/domconfigurator.h>
#include <stdio.h>

#define MAX_LOG_MSG_LEN 512

using namespace lsst::qserv;

void ProtoLog::initLog() {
    // Load XML configuration file using DOMConfigurator
    // TODO: CONFIG FILE PATH NEEDS TO BE DETERMINED DYNAMICALLY.
    log4cxx::xml::DOMConfigurator::configure("/u1/bchick/sandbox2/modules/etc/Log4cxxConfig.xml");
}

void vforcedLog(const log4cxx::LevelPtr &level,
                log4cxx::LoggerPtr logger,
                std::string const& filename,
                std::string const& funcname,
                unsigned int lineno,
                std::string const& fmt,
                va_list args) {
    char msg[MAX_LOG_MSG_LEN];
    vsprintf(msg, fmt.c_str(), args);
    logger->forcedLog(level, msg,
                      log4cxx::spi::LocationInfo(filename.c_str(),
                                                 funcname.c_str(),
                                                 lineno));
}

void ProtoLog::vlog(std::string const& filename, std::string const& funcname,
                    unsigned int lineno, std::string const& loggername,
                    ProtoLog::log_level level, std::string const& fmt,
                    va_list args) {

    log4cxx::LoggerPtr logger;
    if (loggername.empty() || loggername == "root")
        logger = log4cxx::Logger::getRootLogger();
    else
        logger = log4cxx::LoggerPtr(log4cxx::Logger::getLogger(loggername));

    bool (log4cxx::Logger::*checkEnabledFunction)() const = NULL;
    log4cxx::LevelPtr (*getLevelFunction)() = NULL;
    switch (level) {
        case LOG_TRACE:
            checkEnabledFunction = &log4cxx::Logger::isTraceEnabled;
            getLevelFunction = &log4cxx::Level::getTrace;
            break;
        case LOG_DEBUG:
            checkEnabledFunction = &log4cxx::Logger::isDebugEnabled;
            getLevelFunction = &log4cxx::Level::getDebug;
            break;
        case LOG_WARN:
            checkEnabledFunction = &log4cxx::Logger::isWarnEnabled;
            getLevelFunction = &log4cxx::Level::getWarn;
            break;
        case LOG_ERROR:
            checkEnabledFunction = &log4cxx::Logger::isErrorEnabled;
            getLevelFunction = &log4cxx::Level::getError;
            break;
        case LOG_FATAL:
            checkEnabledFunction = &log4cxx::Logger::isFatalEnabled;
            getLevelFunction = &log4cxx::Level::getFatal;
            break;
        default:
            checkEnabledFunction = &log4cxx::Logger::isInfoEnabled;
            getLevelFunction = &log4cxx::Level::getInfo;
            break;
    }
 
    if ((logger->*checkEnabledFunction)())
        vforcedLog((*getLevelFunction)(), logger, filename, funcname, lineno,
                   fmt, args);
        
}

void ProtoLog::log(std::string const& filename, std::string const& funcname,
                   unsigned int lineno, std::string const& loggername,
                   ProtoLog::log_level level, std::string const& fmt, ...) {
    va_list args;
    va_start(args, fmt);
    vlog(filename, funcname, lineno, loggername, level, fmt, args);
}

