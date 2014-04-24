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

// ProtoLog.h:
// class ProtoLog -- A prototype class application-wide logging via log4cxx.
//

#ifndef LSST_QSERV_PROTOLOG_H
#define LSST_QSERV_PROTOLOG_H

#include <string>
#include <stdarg.h>
#include <stack>
#include <log4cxx/logger.h>

// Convenience macros
#define LOG_PUSHCTX(c) lsst::qserv::ProtoLog::pushContext(c)
#define LOG_POPCTX() lsst::qserv::ProtoLog::popContext()

#define LOG_MDC(key, value) lsst::qserv::ProtoLog::MDC(key, value)
#define LOG_MDC_REMOVE(key) lsst::qserv::ProtoLog::MDCRemove(key)

#define LOG(loggername, level, msg...) \
    lsst::qserv::ProtoLog::log(loggername, (*level)(), __BASE_FILE__,\
                               __PRETTY_FUNCTION__, __LINE__, msg)

#define LOG_TRACE(msg...) LOG("", LOG_LVL_TRACE, msg)
#define LOG_DEBUG(msg...) LOG("", LOG_LVL_DEBUG, msg)
#define LOG_INFO(msg...) LOG("", LOG_LVL_INFO, msg)
#define LOG_WARN(msg...) LOG("", LOG_LVL_WARN, msg)
#define LOG_ERROR(msg...) LOG("", LOG_LVL_ERROR, msg)
#define LOG_FATAL(msg...) LOG("", LOG_LVL_FATAL, msg)

#define LOG_LVL_TRACE &log4cxx::Level::getTrace
#define LOG_LVL_DEBUG &log4cxx::Level::getDebug
#define LOG_LVL_INFO &log4cxx::Level::getInfo
#define LOG_LVL_WARN &log4cxx::Level::getWarn
#define LOG_LVL_ERROR &log4cxx::Level::getError
#define LOG_LVL_FATAL &log4cxx::Level::getFatal

namespace lsst {
namespace qserv {

class ProtoLog {
public:
    static void initLog();
    static void pushContext(std::string const& c);
    static void popContext(void);
    static void MDC(std::string const& key, std::string const& value);
    static void MDCRemove(std::string const& key);
    static log4cxx::LoggerPtr getLogger(std::string const& loggername);
    static void log(std::string const& loggername,
                    const log4cxx::LevelPtr &level, std::string const& filename,
                    std::string const& funcname, unsigned int lineno,
                    std::string const& fmt, ...);
    static void vlog(std::string const& loggername,
                     const log4cxx::LevelPtr &level,
                     std::string const& filename, std::string const& funcname,
                     unsigned int lineno, std::string const& fmt, va_list args);
    static void log(log4cxx::LoggerPtr logger, const log4cxx::LevelPtr &level,
                    std::string const& filename, std::string const& funcname,
                    unsigned int lineno, std::string const& fmt, ...);
    static void vlog(log4cxx::LoggerPtr logger, const log4cxx::LevelPtr &level,
                     std::string const& filename, std::string const& funcname,
                     unsigned int lineno, std::string const& fmt, va_list args);
private:
    static std::stack<std::string> context;
    static std::string defaultLogger;
};

}} // lsst::qserv

#endif // LSST_QSERV_PROTOLOG_H
