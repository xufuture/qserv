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
#include "ccontrol/UserQueryFactory.h"

// Qserv headers
#include "ccontrol/ConfigMap.h"
#include "ccontrol/UserQuery.h"
#include "ccontrol/userQueryProxy.h"
#include "css/Facade.h"
#include "qdisp/Executive.h"
#include "qproc/QuerySession.h"

namespace lsst {
namespace qserv {
namespace ccontrol {

class UserQueryFactory::Impl {
public:
    void readConfig(StringMap const& m);
    void initFacade(std::string const& cssTech, std::string const& cssConn);

    qdisp::Executive::Config::Ptr executiveConfig;
    boost::shared_ptr<css::Facade> facade;
};

////////////////////////////////////////////////////////////////////////
UserQueryFactory::UserQueryFactory(StringMap const& m) {
    _impl.reset(new Impl);
    assert(_impl);
    _impl->readConfig(m);
}

int 
UserQueryFactory::newUserQuery(std::string const& query, 
                               std::string const& resultTable) {
    qproc::QuerySession::Ptr qs(new qproc::QuerySession(_impl->facade));
    qs->setResultTable(resultTable);
    qs->setQuery(query);

    UserQuery* uq = new UserQuery(qs);
    int sessionId = UserQuery_takeOwnership(uq);
    uq->_sessionId = sessionId;
    uq->_executive.reset(new qdisp::Executive(_impl->executiveConfig));
    return sessionId;
}

void UserQueryFactory::Impl::readConfig(StringMap const& m) {
    ConfigMap cm(m);
    /// localhost:1094 is the most reasonable default, even though it is
    /// the wrong choice for all but small developer installations.
    std::string serviceUrl = cm.get(
        "frontend.xrootd", // czar.serviceUrl
        "WARNING! No xrootd spec. Using localhost:1094",
        "localhost:1094");
    executiveConfig.reset(new qdisp::Executive::Config(serviceUrl));
#if 0
    _scratchPath =  cm.get(
        "frontend.scratch_path",
        "Error, no scratch path found. Using /tmp.",
        "/tmp");
    // This should be overriden by the installer properly.
    _resultDbSocket =  cm.get(
        "resultdb.unix_socket",
        "Error, resultdb.unix_socket not found. Using /u1/local/mysql.sock.",
        "/u1/local/mysql.sock");
    _resultDbUser =  cm.get(
        "resultdb.user",
        "Error, resultdb.user not found. Using qsmaster.",
        "qsmaster");
    _resultDbDb =  cm.get(
        "resultdb.db",
        "Error, resultdb.db not found. Using qservResult.",
        "qservResult");
#endif
    std::string cssTech = cm.get(
        "css.technology",
        "Error, css.technology not found.",
        "invalid");
    std::string cssConn = cm.get(
        "css.connection",
        "Error, css.connection not found.",
        "");
    initFacade(cssTech, cssConn);
#if 0    
    std::string defaultDb = cm.get(
        "table.defaultdb",
        "Empty table.defaultdb. Using LSST",
        "LSST");
    _qSession->setDefaultDb(defaultDb);
#endif
}
void UserQueryFactory::Impl::initFacade(std::string const& cssTech,
                                        std::string const& cssConn) {
    if (cssTech == "zoo") {
        LOGGER_INF << "Initializing zookeeper-based css, with "
                   << cssConn << std::endl;
        facade = css::FacadeFactory::createZooFacade(cssConn);
//        _qSession.reset(new qproc::QuerySession(cssFPtr));
    } else if (cssTech == "mem") {
        LOGGER_INF << "Initializing memory-based css, with "
                   << cssConn << std::endl;
        facade = css::FacadeFactory::createMemFacade(cssConn);
//        _qSession.reset(new qproc::QuerySession(cssFPtr));
    } else {
        LOGGER_ERR << "Unable to determine css technology, check config file."
                   << std::endl;
//        throw ConfigError("Invalid css technology, check config file.");
// FIXME
    }

}

}}} // lsst::qserv::ccontrol
