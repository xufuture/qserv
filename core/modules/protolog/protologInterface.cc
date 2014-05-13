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
// protologInterface.cc houses the implementation of
// protologInterface.h (SWIG-exported functions for writing to protolog.)

#include "protolog/ProtoLog.h"
#include "protolog/protologInterface.h"
#include <stdarg.h>

namespace qMaster=lsst::qserv::master;

void qMaster::initLog_iface(std::string const& filename) {
    lsst::qserv::ProtoLog::initLog(filename);
}

std::string qMaster::getDefaultLoggerName_iface(void) {
    return lsst::qserv::ProtoLog::getDefaultLoggerName();
}

void qMaster::pushContext_iface(std::string const& name) {
    lsst::qserv::ProtoLog::pushContext(name);
}

void qMaster::popContext_iface() {
    lsst::qserv::ProtoLog::popContext();
}

void qMaster::MDC_iface(std::string const& key, std::string const& value) {
    lsst::qserv::ProtoLog::MDC(key, value);
}

void qMaster::MDCRemove_iface(std::string const& key) {
    lsst::qserv::ProtoLog::MDCRemove(key);
}

void qMaster::setLevel_iface(std::string const& loggername, int level) {
    lsst::qserv::ProtoLog::setLevel(loggername, level);
}

int qMaster::getLevel_iface(std::string const& loggername) {
    return lsst::qserv::ProtoLog::getLevel(loggername);
}

bool qMaster::isEnabledFor_iface(std::string const& loggername, int level) {
    return lsst::qserv::ProtoLog::isEnabledFor(loggername, level);
}

void qMaster::log_iface(std::string const& loggername, int level,
                        std::string const& filename,
                        std::string const& funcname, int lineno,
                        std::string const& msg) {
    lsst::qserv::ProtoLogFormatter(loggername, level, filename, funcname,
                                   lineno, msg.c_str());
}


