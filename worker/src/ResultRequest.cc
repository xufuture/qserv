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
#include "XrdSfs/XrdSfsCallBack.hh"

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
// XrdFinishListener
////////////////////////////////////////////////////////////////////////
class XrdFinishListener { 
public:
    XrdFinishListener(XrdOucErrInfo* eInfo) {
        _callback = XrdSfsCallBack::Create(eInfo);
    }
    virtual void operator()(qWorker::ResultError const& p) {
        if(p.first == 0) {
            // std::cerr << "Callback=OK!\t" << (void*)_callback << std::endl;
            _callback->Reply_OK();
        } else {
            //std::cerr << "Callback error! " << p.first 
            //	      << " desc=" << p.second << std::endl;
            _callback->Reply_Error(p.first, p.second.c_str());
        }
        _callback = 0;
        // _callback will be auto-destructed after any Reply_* call.
    }
private:
    XrdSfsCallBack* _callback;
};
////////////////////////////////////////////////////////////////////////
// NullListener
////////////////////////////////////////////////////////////////////////
class NullListener { 
public:
    explicit NullListener(const char* name) : _name(name) {
    }
    virtual void operator()(qWorker::ResultError const& p) {
        if(p.first == 0) {
            std::cerr << "Callback=OK!\t" << _name << std::endl;
        } else {
            std::cerr << "Callback error! " << _name << " " 
                      << p.first << " desc=" << p.second << std::endl;
        }
    }
private:
    const char* _name;
};
////////////////////////////////////////////////////////////////////////
// class ResultRequest
////////////////////////////////////////////////////////////////////////
qWorker::ResultRequest::ResultRequest(QservPath const& p, XrdOucErrInfo* e) 
    :_state(UNKNOWN), _fsFileEinfo(e) {
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
        if(_fsFileEinfo) {
            qWorker::QueryRunner::getTracker().listenOnce(
                p.hashName(), XrdFinishListener(_fsFileEinfo));
        } else {
            qWorker::QueryRunner::getTracker().listenOnce(
                p.hashName(), NullListener("rrGeneric"));                
        }
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
