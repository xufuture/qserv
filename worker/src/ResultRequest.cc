/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
#include "lsst/qserv/worker/ResultRequest.h"
#include "lsst/qserv/worker/ResultTracker.h"
#include "lsst/qserv/worker/Base.h"
#include "lsst/qserv/worker/QueryRunner.h"
#include "lsst/qserv/QservPath.hh"

namespace qWorker = lsst::qserv::worker;

////////////////////////////////////////////////////////////////////////
// anonymous helpers
////////////////////////////////////////////////////////////////////////
namespace {
#if 0
void lookupResult(std::string const& hash) {
    ResultErrorPtr p = QueryRunner::getTracker().getNews(hash);
}
#endif
}
////////////////////////////////////////////////////////////////////////
// class ResultRequest
////////////////////////////////////////////////////////////////////////
qWorker::ResultRequest::ResultRequest(QservPath const& p) :_state(UNKNOWN) {
    assert(p.requestType() == QservPath::RESULT);
    _accept(p);
}

std::string qWorker::ResultRequest::getStateStr() const {
    std::stringstream ss;
    switch(_state) {
    case UNKNOWN:
        return "Unknown";
    case OPENWAIT:
        return "Waiting for result";
    case OPEN:
        return "Ready";
    case OPENERROR:
        ss << "Error:" << _error;
        return ss.str();
    default:
        return "Corrupt";
    }
}

////////////////////////////////////////////////////////////////////////
// class ResultRequest (private)
////////////////////////////////////////////////////////////////////////
qWorker::ResultRequest::State 
qWorker::ResultRequest::_accept(QservPath const& p) {
    _dumpName = hashToResultPath(p.hashName()); 
    ResultErrorPtr rp = QueryRunner::getTracker().getNews(p.hashName());
    if(rp.get()) {
        if(rp->first != 0) { // Error, so report it.
            _error = rp->second;
            _state = OPENERROR;
        }
    } else { // No news, so listen for it.
        //_addCallback(hash); // FIXME
        _state = OPENWAIT; 
    }
    _state = OPEN;
    return _state;
}

////////////////////////////////////////////////////////////////////////
// class ResultRequest public helper
////////////////////////////////////////////////////////////////////////
std::ostream& 
qWorker::operator<<(std::ostream& os, qWorker::ResultRequest const& rr) {
    os << "ResultRequest " << rr._dumpName << ": " << rr.getStateStr();
    return os;
}
