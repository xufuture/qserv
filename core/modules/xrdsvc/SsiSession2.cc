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
#include "xrdsvc/SsiSession2.h"

// System headers
#include <iostream>
#include <string>

// Third-party
#include "XrdSsi/XrdSsiRequest.hh"

// Qserv headers
#include "global/ResourceUnit.h"
#include "proto/ProtoImporter.h"
#include "proto/worker.pb.h"
#include "wbase/MsgProcessor.h"
#include "wbase/SendChannel.h"
#include "wlog/WLogger.h"

namespace lsst {
namespace qserv {
namespace xrdsvc {

typedef proto::ProtoImporter<proto::TaskMsg> Importer;
typedef boost::shared_ptr<Importer> ImporterPtr;
////////////////////////////////////////////////////////////////////////
// class SsiSession2::ReplyChannel
////////////////////////////////////////////////////////////////////////

class SsiSession2::ReplyChannel : public wbase::SendChannel {
public:
    typedef XrdSsiResponder::Status Status;
    typedef boost::shared_ptr<ReplyChannel> Ptr;

    ReplyChannel(SsiSession2& s) : ssiSession2(s) {}

    virtual void send(char const* buf, int bufLen) {
        Status s = ssiSession2.SetResponse(buf, bufLen);
        if(s != XrdSsiResponder::wasPosted) {
            std::ostringstream os;
            os << "DANGER: Couldn't post response of length="
               << bufLen << std::endl;
            ssiSession2._log->error(os.str());
        }
    }

    virtual void sendError(std::string const& msg, int code) {
        Status s = ssiSession2.SetErrResponse(msg.c_str(), code);
        if(s != XrdSsiResponder::wasPosted) {
            std::ostringstream os;
            os << "DANGER: Couldn't post error response " << msg
               << std::endl;
            ssiSession2._log->error(os.str());
        }
    }
    virtual void sendFile(int fd, Size fSize) {
        Status s = ssiSession2.SetResponse(fSize, fd);
        std::ostringstream os;
        if(s == XrdSsiResponder::wasPosted) {
            os << "file posted ok";
        } else {
            if(s == XrdSsiResponder::notActive) {
                os << "DANGER: Couldn't post response file of length="
                   << fSize << " responder not active.\n";
            } else {
                os << "DANGER: Couldn't post response file of length="
                   << fSize << std::endl;
            }
            release();
            sendError("Internal error posting response file", 1);
        }
        ssiSession2._log->error(os.str());

    }
    SsiSession2& ssiSession2;
};
////////////////////////////////////////////////////////////////////////
// class SsiProcessor
////////////////////////////////////////////////////////////////////////

/// Feed ProtoImporter results to msgprocessor by bundling the responder as a
/// SendChannel
struct SsiProcessor : public Importer::Acceptor {
    typedef boost::shared_ptr<SsiProcessor> Ptr;
    SsiProcessor(ResourceUnit const& ru_,
                 wbase::MsgProcessor::Ptr mp,
                 boost::shared_ptr<wbase::SendChannel> sc)
        : ru(ru_), msgProcessor(mp), sendChannel(sc) {}

    virtual void operator()(boost::shared_ptr<proto::TaskMsg> m) {
        if(m->has_db() && m->has_chunkid()
           && (ru.db() == m->db()) && (ru.chunk() == m->chunkid())) {
            (*msgProcessor)(m, sendChannel);
        } else {
            std::ostringstream os;
            os << "Mismatched db/chunk in msg on resource db="
               << ru.db() << " chunkId=" << ru.chunk();
            sendChannel->sendError(os.str().c_str(), EINVAL);
        }
    }
    ResourceUnit const& ru;
    wbase::MsgProcessor::Ptr msgProcessor;
    boost::shared_ptr<wbase::SendChannel> sendChannel;
};
////////////////////////////////////////////////////////////////////////
// class SsiSession2
////////////////////////////////////////////////////////////////////////

// Step 4
bool
SsiSession2::ProcessRequest(XrdSsiRequest* req, unsigned short timeout) {
    // Figure out what the request is.
    std::ostringstream os;
    os << "ProcessRequest, service=" << sessName;
    _log->info(os.str());

    BindRequest(req, this); // Step 5
    char *reqData = 0;
    int   reqSize;
    reqData = req->GetRequest(reqSize);
    os.str("");
    os << "### " << reqSize <<" byte request: "
       << std::string(reqData, reqSize);
    _log->info(os.str());
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

        enqueue(ru, reqData, reqSize);
        ReleaseRequestBuffer();
    } else {
        // Ignore this request.
        // Should send an error...
        os.str("");
        os << "TODO: Should send an error for Garbage request:"
           << sessName << std::endl;
        _log->info(os.str());
        ReleaseRequestBuffer();
        return false;
    }
    return true;
}

void
SsiSession2::RequestFinished(XrdSsiRequest* req, XrdSsiRespInfo const& rinfo,
                            bool cancel) { // Step 8
    // This call is sync (blocking).
    // client finished retrieving response, or cancelled.
    // release response resources (e.g. buf)

    // No buffers allocated, so don't need to free.
    // We can release/unlink the file now
    std::ostringstream os;
    os << "RequestFinished ";
    switch(rinfo.rType) {
    case XrdSsiRespInfo::isNone: os << "type=isNone"; break;
    case XrdSsiRespInfo::isData: os << "type=isData"; break;
    case XrdSsiRespInfo::isError: os << "type=isError"; break;
    case XrdSsiRespInfo::isFile: os << "type=isFile"; break;
    case XrdSsiRespInfo::isStream: os << "type=isStream"; break;
    }
    // We can't do much other than close the file.
    // It should work (on linux) to unlink the file after we open it, though.
    _log->info(os.str());
}

bool
SsiSession2::Unprovision(bool forced) {
    // all requests guaranteed to be finished or cancelled.
    delete this;
    return true; // false if we can't unprovision now.
}

void SsiSession2::enqueue(ResourceUnit const& ru, char* reqData, int reqSize) {

    // reqData has the entire request, so we can unpack it without waiting for
    // more data.
    ReplyChannel::Ptr rc(new ReplyChannel(*this));
    SsiProcessor::Ptr sp(new SsiProcessor(ru, _processor, rc));
//    Importer::Acceptor imp(new Importer(sp));
    std::ostringstream os;
    os << "Importing TaskMsg of size " << reqSize;
    _log->info(os.str());
    proto::ProtoImporter<proto::TaskMsg> pi(sp);

    pi(reqData, reqSize);
    if(pi.numAccepted() < 1) {
        // TODO Report error.
    } else {
        os.str("");
        os << "enqueued task ok: " << ru;
        _log->error(os.str());

    }
}

}}} // lsst::qserv::xrdsvc

#if 0
// Third-party headers
#include <boost/thread.hpp> // boost::mutex
#include "XrdSsi/XrdSsiResponder.hh"

// Local headers
#include "global/ResourceUnit.h"
#include "wbase/Base.h"

// Forward declarations
// System headers
#include <errno.h>
#include <iostream>
#include <string>

// Third-party headers
#include <boost/shared_ptr.hpp>

// Qserv headers
#include "global/ResourceUnit.h"
#include "wbase/MsgProcessor.h"
#include "wbase/SendChannel.h"

namespace lsst {
namespace qserv {
namespace xrdsvc {

SsiResponder::SsiResponder(boost::shared_ptr<wbase::MsgProcessor> processor)
    :  XrdSsiResponder(this, (void *)11), // 11 = type for responder, not sure
                                          // why we need it.
       _processor(processor) {
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

#endif
