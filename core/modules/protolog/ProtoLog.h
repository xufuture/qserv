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

// ProtoLog.h:
// class ProtoLog -- A prototype class application-wide logging via log4cxx.
//

#ifndef LSST_QSERV_PROTOLOG_H
#define LSST_QSERV_PROTOLOG_H

#include <string>
#include <stack>
#include <log4cxx/logger.h>
#include <boost/format.hpp>

// Convenience macros
#define LOG_INIT(filename) lsst::qserv::ProtoLog::initLog(filename)

#define LOG_DEFAULT_NAME() lsst::qserv::ProtoLog::getDefaultLoggerName()

#define LOG_GET(loggername) lsst::qserv::ProtoLog::getLogger(loggername)

#define LOG_NEWCTX(name) (new lsst::qserv::ProtoLogContext(name))
#define LOG_PUSHCTX(name) lsst::qserv::ProtoLog::pushContext(name)
#define LOG_POPCTX() lsst::qserv::ProtoLog::popContext()

#define LOG_MDC(key, value) lsst::qserv::ProtoLog::MDC(key, value)
#define LOG_MDC_REMOVE(key) lsst::qserv::ProtoLog::MDCRemove(key)

#define LOG_SET_LVL(loggername, level) \
    lsst::qserv::ProtoLog::setLevel(loggername, level)
#define LOG_GET_LVL(loggername) \
    lsst::qserv::ProtoLog::getLevel(loggername)
#define LOG_CHECK_LVL(loggername, level) \
    lsst::qserv::ProtoLog::isEnabledFor(loggername, level)

#define LOG(logger, level, fmt) \
    lsst::qserv::ProtoLogFormatter(logger, level, __BASE_FILE__,\
                                   __PRETTY_FUNCTION__, __LINE__, fmt)

#define LOG_TRACE(fmt) LOG("", LOG_LVL_TRACE, fmt)
#define LOG_DEBUG(fmt) LOG("", LOG_LVL_DEBUG, fmt)
#define LOG_INFO(fmt) LOG("", LOG_LVL_INFO, fmt)
#define LOG_WARN(fmt) LOG("", LOG_LVL_WARN, fmt)
#define LOG_ERROR(fmt) LOG("", LOG_LVL_ERROR, fmt)
#define LOG_FATAL(fmt) LOG("", LOG_LVL_FATAL, fmt)

#define LOG_LOGGER log4cxx::LoggerPtr
#define LOG_CTX lsst::qserv::ProtoLogContext

#define LOG_LVL_TRACE log4cxx::Level::TRACE_INT
#define LOG_LVL_DEBUG log4cxx::Level::DEBUG_INT
#define LOG_LVL_INFO log4cxx::Level::INFO_INT
#define LOG_LVL_WARN log4cxx::Level::WARN_INT
#define LOG_LVL_ERROR log4cxx::Level::ERROR_INT
#define LOG_LVL_FATAL log4cxx::Level::FATAL_INT

namespace lsst {
namespace qserv {

class ProtoLog {
public:
    static void initLog(std::string const& filename);
    static std::string getDefaultLoggerName(void);
    static void pushContext(std::string const& name);
    static void popContext(void);
    static void MDC(std::string const& key, std::string const& value);
    static void MDCRemove(std::string const& key);
    static void setLevel(std::string const& loggername, int level);
    static int getLevel(std::string const& loggername);
    static bool isEnabledFor(std::string const& loggername, int level);
    static log4cxx::LoggerPtr getLogger(std::string const& loggername);
    static void forcedLog(log4cxx::LoggerPtr logger,
                          const log4cxx::LevelPtr &level,
                          std::string const& filename,
                          std::string const& funcname,
                          unsigned int lineno, std::string const& msg);
private:
    static std::stack<std::string> context;
    static std::string defaultLogger;
};

class ProtoLogContext {
public:
    ProtoLogContext(void);
    ProtoLogContext(std::string const& name);
    ~ProtoLogContext(void);
private:
    std::string _name;
};

class ProtoLogFormatter {
public:
    ProtoLogFormatter(std::string const& loggername, int level,
                      std::string const& filename, std::string const& funcname,
                      unsigned int lineno, const char* fmt);
    ProtoLogFormatter(log4cxx::LoggerPtr logger, int level,
                      std::string const& filename, std::string const& funcname,
                      unsigned int lineno, const char* fmt);
    ~ProtoLogFormatter(void);
    template <typename T> ProtoLogFormatter& operator %(T value) {
        _fmter % value;
        return *this;
    }
private:
    log4cxx::LoggerPtr _logger;
    int _level;
    std::string const& _filename;
    std::string const& _funcname;
    unsigned int _lineno;
    boost::format _fmter;
};
    
}} // lsst::qserv

#endif // LSST_QSERV_PROTOLOG_H
