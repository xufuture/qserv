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
#include "lsst/qserv/worker/Foreman.h"
#include "lsst/qserv/worker/Service.h"
#include "lsst/qserv/worker/Logger.h"
#include "lsst/qserv/worker/FifoScheduler.h"
#include "lsst/qserv/worker/ScanScheduler.h"

namespace lsst {
namespace qserv {
namespace worker {

Service::Service(Logger::Ptr log) {
    if(!log.get()) {
        log.reset(new Logger());
    }
    
    Logger::Ptr schedLog(new Logger(log));
    schedLog->setPrefix(ScanScheduler::getName() + ":");
    ScanScheduler::Ptr sch(new ScanScheduler(schedLog));    
    _foreman = newForeman(sch, log);
}

TaskAcceptor::Ptr Service::getAcceptor() {
    return _foreman;
}

void Service::squashByHash(std::string const& hash) {
    _foreman->squashByHash(hash);
}

}}} // lsst::qserv::worker
