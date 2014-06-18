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
// class Executive is in charge of "executing" user query fragments on
// a qserv cluster.

#ifndef LSST_QSERV_QDISP_EXECUTIVE_H
#define LSST_QSERV_QDISP_EXECUTIVE_H

// Third-party headers
#include <boost/thread.hpp> // boost::mutex
//#include "XrdPosix/XrdPosixCallBack.hh"

// Local headers
//#include "util/Timer.h"
//#include "xrdc/xrdfile.h"
#include "global/ResourceUnit.h"
#include "global/stringTypes.h"
#include "qdisp/TransactionSpec.h"

// Forward declarations
class XrdSsiService;

namespace lsst {
namespace qserv {
namespace control {
    class AsyncQueryManager;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace qdisp {
class QueryReceiver;

//////////////////////////////////////////////////////////////////////
// class Executive
//////////////////////////////////////////////////////////////////////
class Executive {
public:
    typedef boost::shared_ptr<Executive> Ptr;
    struct Config {
        typedef boost::shared_ptr<Config> Ptr;
        Config(std::string const& serviceUrl_)
            : serviceUrl(serviceUrl_) {}
        std::string serviceUrl;
    };
    struct Spec {
        ResourceUnit resource; // path, e.g. /q/LSST/23125
        std::string request; // encoded request
        boost::shared_ptr<QueryReceiver> receiver;
    };


    Executive(Config::Ptr c) : _config(*c) { _setup(); }
    void abort();
    void add(int refNum,
             TransactionSpec const& t, std::string const& resultName);
    void add(int refNum, Spec const& s);
    bool join();
    void remove(int refNum);
    void requestSquash(int refNum);

    // FIXME
    std::string getProgressDesc() const { return std::string ("it's fine"); }


private:
    typedef boost::shared_ptr<QueryReceiver> ReceiverPtr;
    typedef std::map<int, ReceiverPtr> ReceiverMap;

    void _setup();
    bool _track(int refNum, ReceiverPtr r);
    void _unTrack(int refNum);
    void _waitUntilEmpty();
    void _reapReceivers(boost::unique_lock<boost::mutex> const& receiversLock);

    // for debugging
    void _printState(std::ostream& os);


    Config _config; // Personal copy of config
    XrdSsiService* _service;
    ReceiverMap _receivers;
    int _requestCount;

    // Mutexes
    boost::mutex _receiversMutex;
    boost::condition_variable _receiversEmpty;
    boost::mutex _requestsMutex;

}; // class Executive
}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_EXECUTIVE_H
