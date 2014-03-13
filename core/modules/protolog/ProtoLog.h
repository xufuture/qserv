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

#define LOG_TRACE(msg...) LOG("root", lsst::qserv::ProtoLog::LOG_TRACE, msg)
#define LOG_DEBUG(msg...) LOG("root", lsst::qserv::ProtoLog::LOG_DEBUG, msg)
#define LOG_INFO(msg...) LOG("root", lsst::qserv::ProtoLog::LOG_INFO, msg)
#define LOG_WARN(msg...) LOG("root", lsst::qserv::ProtoLog::LOG_WARN, msg)
#define LOG_ERROR(msg...) LOG("root", lsst::qserv::ProtoLog::LOG_ERROR, msg)
#define LOG_FATAL(msg...) LOG("root", lsst::qserv::ProtoLog::LOG_FATAL, msg)
#define LOG(loggername, level, msg...) \
    lsst::qserv::ProtoLog::log(__FILE__, __PRETTY_FUNCTION__, __LINE__,\
                               loggername, level, msg);

namespace lsst {
namespace qserv {

class ProtoLog {
public:
    enum log_level {
        LOG_TRACE = 0,
        LOG_DEBUG,
        LOG_INFO,
        LOG_WARN,
        LOG_ERROR,
        LOG_FATAL
    };
    static void initLog();
    static void log(std::string const& filename, std::string const& funcname,
                    unsigned int lineno, std::string const& loggername,
                    log_level level, std::string const& fmt, ...);
    static void vlog(std::string const& filename, std::string const& funcname,
                     unsigned int lineno, std::string const& loggername,
                     log_level level, std::string const& fmt, va_list args);
};

}} // lsst::qserv

#endif // LSST_QSERV_PROTOLOG_H
