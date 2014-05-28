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
#include <stdarg.h>
#include <stack>
#include <log4cxx/logger.h>
#include <boost/format.hpp>

// Convenience macros
#define LOG_INIT(filename) lsst::qserv::ProtoLog::initLog(filename)

#define LOG_DEFAULT_NAME() lsst::qserv::ProtoLog::getDefaultLoggerName()

#define LOG_GET(logger) lsst::qserv::ProtoLog::getLogger(logger)

#define LOG_NEWCTX(name) (new lsst::qserv::ProtoLogContext(name))
#define LOG_PUSHCTX(name) lsst::qserv::ProtoLog::pushContext(name)
#define LOG_POPCTX() lsst::qserv::ProtoLog::popContext()

#define LOG_MDC(key, value) lsst::qserv::ProtoLog::MDC(key, value)
#define LOG_MDC_REMOVE(key) lsst::qserv::ProtoLog::MDCRemove(key)

#define LOG_SET_LVL(logger, level) \
    lsst::qserv::ProtoLog::setLevel(logger, level)
#define LOG_GET_LVL(logger) \
    lsst::qserv::ProtoLog::getLevel(logger)
#define LOG_CHECK_LVL(logger, level) \
    lsst::qserv::ProtoLog::isEnabledFor(logger, level)

// boost::format style logging

#define LOGF(logger, level, fmt) \
    if (lsst::qserv::ProtoLog::isEnabledFor(logger, level)) { \
        lsst::qserv::ProtoLogFormatter fmter_; \
        lsst::qserv::ProtoLog::getLogger(logger)->forcedLog( \
            log4cxx::Level::toLevel(level), (fmter_ % fmt).str().c_str(), \
            LOG4CXX_LOCATION); }
#define LOGF_TRACE(fmt) \
    if (lsst::qserv::ProtoLog::defaultLogger->isTraceEnabled()) { \
        lsst::qserv::ProtoLogFormatter fmter_; \
        lsst::qserv::ProtoLog::defaultLogger->forcedLog( \
            log4cxx::Level::getTrace(), (fmter_ % fmt).str().c_str(), \
            LOG4CXX_LOCATION); }
#define LOGF_DEBUG(fmt) \
    if (lsst::qserv::ProtoLog::defaultLogger->isDebugEnabled()) { \
        lsst::qserv::ProtoLogFormatter fmter_; \
        lsst::qserv::ProtoLog::defaultLogger->forcedLog( \
            log4cxx::Level::getDebug(), (fmter_ % fmt).str().c_str(), \
            LOG4CXX_LOCATION); }
#define LOGF_INFO(fmt) \
    if (lsst::qserv::ProtoLog::defaultLogger->isInfoEnabled()) { \
        lsst::qserv::ProtoLogFormatter fmter_; \
        lsst::qserv::ProtoLog::defaultLogger->forcedLog( \
            log4cxx::Level::getInfo(), (fmter_ % fmt).str().c_str(), \
            LOG4CXX_LOCATION); }
#define LOGF_WARN(fmt) \
    if (lsst::qserv::ProtoLog::defaultLogger->isWarnEnabled()) { \
        lsst::qserv::ProtoLogFormatter fmter_; \
        lsst::qserv::ProtoLog::defaultLogger->forcedLog( \
            log4cxx::Level::getWarn(), (fmter_ % fmt).str().c_str(), \
            LOG4CXX_LOCATION); }
#define LOGF_ERROR(fmt) \
    if (lsst::qserv::ProtoLog::defaultLogger->isErrorEnabled()) { \
        lsst::qserv::ProtoLogFormatter fmter_; \
        lsst::qserv::ProtoLog::defaultLogger->forcedLog( \
            log4cxx::Level::getError(), (fmter_ % fmt).str().c_str(), \
            LOG4CXX_LOCATION); }
#define LOGF_FATAL(fmt) \
    if (lsst::qserv::ProtoLog::defaultLogger->isFatalEnabled()) { \
        lsst::qserv::ProtoLogFormatter fmter_; \
        lsst::qserv::ProtoLog::defaultLogger->forcedLog( \
            log4cxx::Level::getFatal(), (fmter_ % fmt).str().c_str(), \
            LOG4CXX_LOCATION); }

// varargs logging API

#define LOG(logger, level, fmt...) \
    if (lsst::qserv::ProtoLog::isEnabledFor(logger, level)) { \
        lsst::qserv::ProtoLog::log(logger, log4cxx::Level::toLevel(level), \
        __BASE_FILE__, __PRETTY_FUNCTION__, __LINE__, fmt); }
#define LOG_TRACE(fmt...) \
    if (lsst::qserv::ProtoLog::defaultLogger->isTraceEnabled()) { \
        lsst::qserv::ProtoLog::log(lsst::qserv::ProtoLog::defaultLogger, \
            log4cxx::Level::getTrace(), __BASE_FILE__, __PRETTY_FUNCTION__, \
            __LINE__, fmt); }
#define LOG_DEBUG(fmt...) \
    if (lsst::qserv::ProtoLog::defaultLogger->isDebugEnabled()) { \
        lsst::qserv::ProtoLog::log(lsst::qserv::ProtoLog::defaultLogger, \
            log4cxx::Level::getDebug(), __BASE_FILE__, __PRETTY_FUNCTION__, \
            __LINE__, fmt); }
#define LOG_INFO(fmt...) \
    if (lsst::qserv::ProtoLog::defaultLogger->isInfoEnabled()) { \
        lsst::qserv::ProtoLog::log(lsst::qserv::ProtoLog::defaultLogger, \
            log4cxx::Level::getInfo(), __BASE_FILE__, __PRETTY_FUNCTION__, \
            __LINE__, fmt); }
#define LOG_WARN(fmt...) \
    if (lsst::qserv::ProtoLog::defaultLogger->isWarnEnabled()) { \
        lsst::qserv::ProtoLog::log(lsst::qserv::ProtoLog::defaultLogger, \
            log4cxx::Level::getWarn(), __BASE_FILE__, __PRETTY_FUNCTION__, \
            __LINE__, fmt); }
#define LOG_ERROR(fmt...) \
    if (lsst::qserv::ProtoLog::defaultLogger->isErrorEnabled()) { \
        lsst::qserv::ProtoLog::log(lsst::qserv::ProtoLog::defaultLogger, \
            log4cxx::Level::getError(), __BASE_FILE__, __PRETTY_FUNCTION__, \
            __LINE__, fmt); }
#define LOG_FATAL(fmt...) \
    if (lsst::qserv::ProtoLog::defaultLogger->isFatalEnabled()) { \
        lsst::qserv::ProtoLog::log(lsst::qserv::ProtoLog::defaultLogger, \
            log4cxx::Level::getFatal(), __BASE_FILE__, __PRETTY_FUNCTION__, \
            __LINE__, fmt); }

#define LOG_LVL_TRACE log4cxx::Level::TRACE_INT
#define LOG_LVL_DEBUG log4cxx::Level::DEBUG_INT
#define LOG_LVL_INFO log4cxx::Level::INFO_INT
#define LOG_LVL_WARN log4cxx::Level::WARN_INT
#define LOG_LVL_ERROR log4cxx::Level::ERROR_INT
#define LOG_LVL_FATAL log4cxx::Level::FATAL_INT

#define LOG_LOGGER log4cxx::LoggerPtr
#define LOG_CTX lsst::qserv::ProtoLogContext

namespace lsst {
namespace qserv {

class ProtoLogFormatter {
public:
    ProtoLogFormatter(void);
    ~ProtoLogFormatter(void);
    template <typename T> boost::format& operator %(T fmt) {
        _enabled = true;
        _fmter = new boost::format(fmt);
        return *_fmter;
    }
private:
    bool _enabled;
    boost::format* _fmter;
};

class ProtoLogContext {
public:
    ProtoLogContext(void);
    ProtoLogContext(std::string const& name);
    ~ProtoLogContext(void);
private:
    std::string _name;
};

class ProtoLog {
public:
    static log4cxx::LoggerPtr defaultLogger;
    static void initLog(std::string const& filename);
    static std::string getDefaultLoggerName(void);
    static log4cxx::LoggerPtr getLogger(log4cxx::LoggerPtr logger);
    static log4cxx::LoggerPtr getLogger(std::string const& loggername);
    static void pushContext(std::string const& name);
    static void popContext(void);
    static void MDC(std::string const& key, std::string const& value);
    static void MDCRemove(std::string const& key);
    static void setLevel(log4cxx::LoggerPtr logger, int level);
    static void setLevel(std::string const& loggername, int level);
    static int getLevel(log4cxx::LoggerPtr logger);
    static int getLevel(std::string const& loggername);
    static bool isEnabledFor(log4cxx::LoggerPtr logger, int level);
    static bool isEnabledFor(std::string const& loggername, int level);
    // varargs/printf style logging
    static void vlog(log4cxx::LoggerPtr logger, log4cxx::LevelPtr level,
                     std::string const& filename, std::string const& funcname,
                     unsigned int lineno, std::string const& fmt, va_list args);
    static void log(std::string const& loggername, log4cxx::LevelPtr level,
                    std::string const& filename, std::string const& funcname,
                    unsigned int lineno, std::string const& fmt, ...);
    static void log(log4cxx::LoggerPtr logger, log4cxx::LevelPtr level,
                    std::string const& filename, std::string const& funcname,
                    unsigned int lineno, std::string const& fmt, ...);
private:
    static std::stack<std::string> context;
    static std::string defaultLoggerName;
};

}} // lsst::qserv

#endif // LSST_QSERV_PROTOLOG_H
