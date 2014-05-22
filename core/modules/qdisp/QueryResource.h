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
#ifndef LSST_QSERV_QDISP_QUERYRESOURCE_H
#define LSST_QSERV_QDISP_QUERYRESOURCE_H

// System headers
#include <string>

// Third-party headers
//#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include "XrdSsi/XrdSsiService.hh" // Resource

// Local headers
//#include "ccontrol/transaction.h"
//#include "util/Timer.h"
//#include "xrdc/xrdfile.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace ccontrol {
//    class AsyncQueryManager;
}
namespace xrdc {
//    class PacketIter;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace qdisp {
class QueryReceiver;
class QueryRequest;

/// Note: this object takes responsibility for deleting itself once it is passed
/// off via service->Provision(resourceptr).
class QueryResource : public XrdSsiService::Resource {
public:
    /// @param rPath resource path, e.g. /LSST/12312
    QueryResource(std::string const& rPath, 
                  std::string const& payload, 
                  boost::shared_ptr<QueryReceiver> receiver)
        : Resource(rPath.c_str()),
          _payload(payload),
          _receiver(receiver) {
    }

    virtual ~QueryResource() {}

    void ProvisionDone(XrdSsiSession* s);

    XrdSsiSession* _session; // unowned, do not delete.
    QueryRequest* _request; // Owned temporarily, special deletion handling.
    std::string const _payload;
    boost::shared_ptr<QueryReceiver> _receiver;
};

}}} // namespace lsst::qserv::qdisp

#endif // LSST_QSERV_QDISP_QUERYRESOURCE_H
