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
#include <errno.h>
#include <iostream>
#include <string>

// Third-party headers
#include <boost/shared_ptr.hpp>

// Qserv headers
#include "global/ResourceUnit.h"
#include "proto/ProtoImporter.h"
#include "proto/worker.pb.h"
#include "wbase/MsgProcessor.h"
#include "wbase/SendChannel.h"

namespace lsst {
namespace qserv {
namespace xrdsvc {
typedef proto::ProtoImporter<proto::TaskMsg> Importer;
typedef boost::shared_ptr<Importer> ImporterPtr;

class SsiResponder::ReplyChannel : public wbase::SendChannel {
public:
    typedef XrdSsiResponder::Status Status;
    typedef boost::shared_ptr<ReplyChannel> Ptr;

    ReplyChannel(SsiResponder& sr) : ssiResponder(sr) {}

    virtual void send(char const* buf, int bufLen) {
        Status s = ssiResponder.SetResponse(buf, bufLen);
        if(s != XrdSsiResponder::wasPosted) {
            std::cerr << "DANGER: Couldn't post response of length="
                      << bufLen << std::endl;
        }
    }

    virtual void sendError(std::string const& msg, int code) {
        Status s = ssiResponder.SetErrResponse(msg.c_str(), code);
        if(s != XrdSsiResponder::wasPosted) {
            std::cerr << "DANGER: Couldn't post error response " << msg
                      << std::endl;
        }
    }
    virtual void sendFile(int fd, Size fSize) {
        Status s = ssiResponder.SetResponse(fSize, fd);
        if(s != XrdSsiResponder::wasPosted) {
            std::cerr << "DANGER: Couldn't post response file of length="
                      << fSize << std::endl;
        } else if(s == XrdSsiResponder::notActive) {
            std::cerr << "DANGER: Couldn't post response file of length="
                      << fSize << " responder not active.\n";
        }

    }
    SsiResponder& ssiResponder;
};

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

SsiResponder::SsiResponder(boost::shared_ptr<wbase::MsgProcessor> processor)
    :  XrdSsiResponder(this, (void *)11), // 11 = type for responder, not sure
                                          // why we need it.
       _processor(processor) {
}

void SsiResponder::enqueue(ResourceUnit const& ru, char* reqData, int reqSize) {
#if 1
    // reqData has the entire request, so we can unpack it without waiting for
    // more data.
    ReplyChannel::Ptr rc(new ReplyChannel(*this));
    SsiProcessor::Ptr sp(new SsiProcessor(ru, _processor, rc));
//    Importer::Acceptor imp(new Importer(sp));
    std::cout << "Importing TaskMsg of size " << reqSize << std::endl;
    proto::ProtoImporter<proto::TaskMsg> pi(sp);

    pi(reqData, reqSize);
    if(pi.numAccepted() < 1) {
        // TODO Report error.
    } else {
        std::cerr << "enqueued task ok: " << ru << std::endl;
    }
#else
// TODO
    _requestTaker.reset(new wcontrol::RequestTaker(_service->getAcceptor(),
                                                   *_path));
    _chunkId = -1; // unused.
#endif
    ReleaseRequestBuffer();
}
#if 0
void SsiResponder::doStuff() {
    // TODO
    // Compute request and place it in the scheduler, then release the request buffer.
    // Make sure that request knows how to fire the response: place responding code into a callback so we can put it in the scheduler's task.

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
}
#endif

}}} // lsst::qserv::xrdsvc
