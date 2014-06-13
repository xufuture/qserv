// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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
#include "xrdsvc/SsiSession.h"

// System headers
#include <iostream>
#include <string>

// Third-party
#include "XrdSsi/XrdSsiRequest.hh"

// Qserv headers
#include "wlog/WLogger.h"
#include "xrdsvc/SsiResponder.h"


class XrdPosixCallBack; // Forward.

namespace lsst {
namespace qserv {
namespace xrdsvc {

// Step 4
bool
SsiSession::ProcessRequest(XrdSsiRequest* req, unsigned short timeout) {
#if 1
    // Figure out what the request is.
    std::ostringstream os;
    os << "ProcessRequest, service=" << sessName;
    _log->info(os.str());

    char *reqData = 0;
    int   reqSize;
    reqData = req->GetRequest(reqSize);
    os.str("");
    os << "### " << reqSize <<" byte request: "
       << std::string(reqData, reqSize);
    _log->info(os.str());
    std::cerr  << "ERRSTREAM" << os.str() << std::endl;
    SsiResponder* resp = new SsiResponder(_processor);
    resp->TakeRequest(req, this, resp); // Step 5
    ResourceUnit ru(sessName);
    if(ru.unitType() == ResourceUnit::DBCHUNK) {
        if(!(*_validator)(ru)) {
            os.str("");
            os << "WARNING: unowned chunk query detected: "
               << ru.path();
            _log->warn(os.str());
            //error.setErrInfo(ENOENT, "File does not exist");
            return false;
        }

        resp->enqueue(ru, reqData, reqSize);
        // Record responder so we can get in touch (for squashing/cleanup)
        _responders.push_back(boost::shared_ptr<SsiResponder>(resp));
    } else {
        // Ignore this request.
        // Should send an error...
        os.str("");
        os << "TODO: Should send an error for Garbage request:"
           << sessName << std::endl;
        _log->info(os.str());
        delete resp;
        return false;
    }
    return true;
#else
    char *reqData, *reqArgs = 0;
    int   reqSize, fd, n;

    reqData = req->GetRequest(reqSize);
    std::ostringstream ss;
    ss << "### " << reqSize <<" byte request: "
       << std::string(reqData, reqSize) << std::endl;
    std::cerr << ss.str();

    bool wantToProcess = true;
    // Thread spawned for this call
    if(wantToProcess) {
        // Qserv: put work on queue (probably session and responder together)
        SsiResponder* resp = new SsiResponder();
        resp->TakeRequest(req, this, resp); // Step 5
        resp->doStuff(); // Trigger something to get work done.
        // When can I delete MyResponder?
        delete resp; // will this work?
        return true;
    } else {
        //req->SetErrInfo(/* ... */);
        return false;
    }
#endif
}

void
SsiSession::RequestFinished(XrdSsiRequest* req, XrdSsiRespInfo const& rinfo,
                            bool cancel) { // Step 8
    // This call is sync (blocking).
    // client finished retrieving response, or cancelled.
    // release response resources (e.g. buf)

    // No buffers allocated, so don't need to free.
}

bool
SsiSession::Unprovision(bool forced) {
    // all requests guaranteed to be finished or cancelled.
    delete this;
    return true; // false if we can't unprovision now.
}

}}} // lsst::qserv::xrdsvc
