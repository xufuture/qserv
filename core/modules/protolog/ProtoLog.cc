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
    vsnprintf(msg, MAX_LOG_MSG_LEN, fmt.c_str(), args);
    logger->forcedLog(level, msg,
                      log4cxx::spi::LocationInfo(filename.c_str(),
                                                 funcname.c_str(),
                                                 lineno));
}

void ProtoLog::vlog(std::string const& filename, std::string const& funcname,
                    unsigned int lineno, std::string const& loggername,
                    const log4cxx::LevelPtr &level, std::string const& fmt,
                    va_list args) {
    // Retrieve logger object
    log4cxx::LoggerPtr logger;
    if (loggername.empty() || loggername == "root")
        logger = log4cxx::Logger::getRootLogger();
    else
        logger = log4cxx::Logger::getLogger(loggername);
    // Check logging level
    if (logger->isEnabledFor(level))
        vforcedLog(level, logger, filename, funcname, lineno, fmt, args);
}

void ProtoLog::log(std::string const& filename, std::string const& funcname,
                   unsigned int lineno, std::string const& loggername,
                   const log4cxx::LevelPtr &level, std::string const& fmt,
                   ...) {
    va_list args;
    va_start(args, fmt);
    vlog(filename, funcname, lineno, loggername, level, fmt, args);
}

