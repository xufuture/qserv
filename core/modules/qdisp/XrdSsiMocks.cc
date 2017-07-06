// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
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
 *
 * @author John Gates, SLAC
 */

// System headers
#include <cstddef>
#include <string>
#include <stdlib.h>
#include <thread>
#include <unistd.h>

// LSST headers
#include "lsst/log/Log.h"

// Local headers
#include "qdisp/Executive.h"
#include "qdisp/XrdSsiMocks.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.XrdSsiMock");

class Agent : public XrdSsiResponder {
public:
    void ReplyErr() {
        BindRequest(*_reqP);
        SetErrResponse("Mock Request Ignored!", 17);
    }

    void Finished(XrdSsiRequest&        rqstR,
                  XrdSsiRespInfo const& rInfo,
                  bool cancel) {UnBindRequest(); delete this;}

    Agent(lsst::qserv::qdisp::QueryRequest* rP) : _reqP(rP) {}
    ~Agent() {}

private:
    lsst::qserv::qdisp::QueryRequest* _reqP;
};
}

namespace lsst {
namespace qserv {
namespace qdisp {

util::FlagNotify<bool> XrdSsiServiceMock::_go(true);
util::Sequential<int> XrdSsiServiceMock::_count(0);


void XrdSsiServiceMock::ProcessRequest(XrdSsiRequest  &reqRef,
                                       XrdSsiResource &resRef) {
    LOGS(_log, LOG_LVL_DEBUG, "rName=" << resRef.rName);
    _count.incr();
    // Normally, reqP->ProcessResponse() would be called, which invokes
    // cleanup code that is necessary to avoid memory leaks. Instead,
    // clean up the request manually. First, cancel the query because if
    // we don't cleanup() will fail the mock test will fail as well.
    QueryRequest * r = dynamic_cast<QueryRequest *>(&reqRef);
    if (r) {
        Agent* aP = new Agent(r);
        r->doNotRetry();
        std::thread (&Agent::ReplyErr, aP).detach();
//      r->getStatus()->updateInfo(JobStatus::RESPONSE_DONE);
//      r->cleanup();
    }
}

void XrdSsiServiceMock::Finished(XrdSsiRequest&        rqstR,
                                 XrdSsiRespInfo const& rInfo,
                                 bool cancel) {}

}}} // namespace
