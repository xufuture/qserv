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
#include "xrdsvc/SsiResponder.h"

// System headers
#include <string>

// Qserv headers
#include "global/ResourceUnit.h"

namespace lsst {
namespace qserv {
namespace xrdsvc {

void SsiResponder::enqueue(ResourceUnit const& ru, char* reqData, int reqSize) {
#if 0
// TODO
    _requestTaker.reset(new wcontrol::RequestTaker(_service->getAcceptor(),
                                                   *_path));
    _chunkId = -1; // unused.
#endif    
    ReleaseRequestBuffer();
}

void SsiResponder::doStuff() {
#if 1
    // TODO
    // Compute request and place it in the scheduler, then release the request buffer.
    // Make sure that request knows how to fire the response: place responding code into a callback so we can put it in the scheduler's task.

#else
    // I don't need the request buffer anymore, so delete it.
    ReleaseRequestBuffer();

    // action when it's my turn on the work queue
    bool noError = true;
    if(noError) { 
        // buf allocated by session obj.
        char* b = (char*)response.data();
        int blen = response.length();
        stringstream ss;
        ss << "Sending b=" << (void*)b << " len=" << blen << endl;
        cerr << ss.str();
        Status s = SetResponse(b, blen); // Step 6
        ReleaseRequestBuffer(); // Release reqP
        ss << "SetResponse returned ";
        switch(s) {
        case wasPosted: ss << "wasPosted"; break;
        case notPosted: ss << "notPosted"; break;
        case notActive: ss << "notActive"; break;
        default: ss << "______"; break;
        }
        ss << endl;
        cerr << ss.str();
    } else {
        //SetResponse(filedesc);
        //SetResponse(XrdSsiStream)
                
    }
#endif
}

}}} // lsst::qserv::xrdsvc
