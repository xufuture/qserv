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

#ifndef LSST_QSERV_XRDSVC_SSISESSION_H
#define LSST_QSERV_XRDSVC_SSISESSION_H

// Third-party headers
#include <boost/thread.hpp> // boost::mutex
#include "XrdSsi/XrdSsiSession.hh"

// Local headers
#include "global/ResourceUnit.h"

// Forward declarations
class XrdSsiService;

namespace lsst {
namespace qserv {
namespace wlog {
class WLogger;
}}}

namespace lsst {
namespace qserv {
namespace xrdsvc {

class SsiSession : public XrdSsiSession {
public:
    typedef boost::shared_ptr<ResourceUnit::Checker> ValidatorPtr;

    SsiSession(char const* sname, 
               ValidatorPtr v, 
               boost::shared_ptr<wlog::WLogger> log)
        : XrdSsiSession(strdup(sname), 0),
          _validator(v),
          _log(log)
        {}

    virtual ~SsiSession() {
        // XrdSsiSession::sessName is unmanaged
        if(sessName) { ::free(sessName); sessName = 0; }
        // delete my own resources.
    }
    virtual bool ProcessRequest(XrdSsiRequest* req, unsigned short timeout);
    virtual void RequestFinished(XrdSsiRequest* req, XrdSsiRespInfo const& rinfo,
                                 bool cancel=false);

    virtual bool Unprovision(bool forced);
private:
    ValidatorPtr _validator;
    boost::shared_ptr<wlog::WLogger> _log;
}; // class SsiSession
}}} // namespace lsst::qserv::xrdsvc

#endif // LSST_QSERV_XRDSVC_SSISESSION_H

