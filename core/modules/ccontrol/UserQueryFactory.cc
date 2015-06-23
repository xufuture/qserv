// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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

// Class header
#include "ccontrol/UserQueryFactory.h"

// System headers
#include <cassert>
#include <sstream>
#include <stdlib.h>

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/ConfigMap.h"
#include "ccontrol/ConfigError.h"
#include "ccontrol/UserQuery.h"
#include "ccontrol/userQueryProxy.h"
#include "css/Facade.h"
#include "css/KvInterfaceImplMem.h"
#include "mysql/MySqlConfig.h"
#include "qdisp/Executive.h"
#include "qproc/QuerySession.h"
#include "qproc/SecondaryIndex.h"
#include "rproc/InfileMerger.h"
#include "rproc/TableMerger.h"

namespace lsst {
namespace qserv {
namespace ccontrol {

LOG_LOGGER UserQueryFactory::_log = LOG_GET("lsst.qserv.ccontrol.UserQueryFactory");

/// Implementation class (PIMPL-style) for UserQueryFactory.
class UserQueryFactory::Impl {
public:
    /// Import config from caller
    void readConfig(StringMap const& m);

    void createFacade(std::shared_ptr<css::KvInterface> kvi);
    void purgeFacades();
    int cssRefreshFreq() const {return _cssRefreshFreq;}

    void initMergerTemplate(); ///< Construct template config for merger

    /// State shared between UserQueries
    qdisp::Executive::Config::Ptr executiveConfig;
    rproc::InfileMergerConfig infileMergerConfigTemplate;
    std::shared_ptr<qproc::SecondaryIndex> secondaryIndex;
    std::vector<std::shared_ptr<css::Facade>> facades;

private:
    std::string _emptyChunkPath;
    std::string _cssTech;
    std::string _cssConn;
    int _cssRefreshFreq;
};

////////////////////////////////////////////////////////////////////////
UserQueryFactory::UserQueryFactory(StringMap const& m,
                                   std::shared_ptr<css::KvInterface> kvi)
    :  _impl(std::make_shared<Impl>()) {
    ::putenv((char*)"XRDDEBUG=1");
    assert(_impl);
    _impl->readConfig(m);
    _impl->createFacade(kvi);
}

void
UserQueryFactory::createFacade(std::shared_ptr<css::KvInterface> kvi) {
    _impl->createFacade(kvi);
}

void
UserQueryFactory::purgeFacades() {
    _impl->purgeFacades();
}

int
UserQueryFactory::cssRefreshFreq() const {
    return _impl->cssRefreshFreq();
}

int
UserQueryFactory::newUserQuery(std::string const& query,
                               std::string const& defaultDb,
                               std::string const& resultTable) {
    bool sessionValid = true;
    std::string errorExtra;
    // Use the last facade, it has the most recent snapshot
    LOGF(_log, LOG_LVL_DEBUG,
         "Using CSS Facade %1%" % _impl->facades.back().get());
    qproc::QuerySession::Ptr qs =
        std::make_shared<qproc::QuerySession>(_impl->facades.back());
    try {
        qs->setResultTable(resultTable);
        qs->setDefaultDb(defaultDb);
        qs->setQuery(query);
    } catch (...) {
        errorExtra = "Unknown failure when creating QuerySession (query is invalid).";
        LOGF(_log, LOG_LVL_ERROR, errorExtra);
        sessionValid = false;
    }
    if(!qs->getError().empty()) {
        LOGF(_log, LOG_LVL_ERROR, "Invalid query: %s" % qs->getError());
        sessionValid = false;
    }
    UserQuery* uq = new UserQuery(qs);
    uq->_sessionId = UserQuery_takeOwnership(uq);
    uq->_secondaryIndex = _impl->secondaryIndex;
    if(sessionValid) {
        uq->_executive = std::make_shared<qdisp::Executive>(
                _impl->executiveConfig, uq->_messageStore);

        rproc::InfileMergerConfig* ict
            = new rproc::InfileMergerConfig(_impl->infileMergerConfigTemplate);
        ict->targetTable = resultTable;
        uq->_infileMergerConfig.reset(ict);
        uq->_setupChunking();
    } else {
        uq->_errorExtra += errorExtra;
    }
    return uq->_sessionId;
}

void UserQueryFactory::Impl::readConfig(StringMap const& m) {
    ConfigMap cm(m);
    /// localhost:1094 is the most reasonable default, even though it is
    /// the wrong choice for all but small developer installations.
    std::string serviceUrl = cm.get(
        "frontend.xrootd", // czar.serviceUrl
        "WARNING! No xrootd spec. Using localhost:1094",
        "localhost:1094");
    executiveConfig = std::make_shared<qdisp::Executive::Config>(serviceUrl);
    // This should be overriden by the installer properly.
    infileMergerConfigTemplate.socket = cm.get(
        "resultdb.unix_socket",
        "Error, resultdb.unix_socket not found. Using /u1/local/mysql.sock.",
        "/u1/local/mysql.sock");
    infileMergerConfigTemplate.user = cm.get(
        "resultdb.user",
        "Error, resultdb.user not found. Using qsmaster.",
        "qsmaster");
    infileMergerConfigTemplate.targetDb = cm.get(
        "resultdb.db",
        "Error, resultdb.db not found. Using qservResult.",
        "qservResult");
    mysql::MySqlConfig mc;
    mc.username = infileMergerConfigTemplate.user;
    mc.dbName = infileMergerConfigTemplate.targetDb; // any valid db is ok.
    mc.socket = infileMergerConfigTemplate.socket;
    secondaryIndex = std::make_shared<qproc::SecondaryIndex>(mc);

    _emptyChunkPath = cm.get(
        "partitioner.emptychunkpath",
        "Error, missing path for Empty chunk file, using '.'.",
        ".");
    _cssTech = cm.get("css.technology",
                      "Error, css.technology not found.",
                      "invalid");
    _cssConn = cm.get("css.connection",
                      "Error, css.connection not found.",
                      "");
    std::string rfStr =
        cm.get("css.refreshFrequency",
               "Warning, css.refreshFrequency not found, using 15 sec",
               "15");
    std::istringstream (rfStr) >> _cssRefreshFreq;
}

void
UserQueryFactory::Impl::createFacade(std::shared_ptr<css::KvInterface> kvi) {
    if (kvi) {
        LOGF(_log, LOG_LVL_DEBUG, "Creating CSS CacheFacade");
        facades.push_back(
            css::FacadeFactory::createCacheFacade(kvi, _emptyChunkPath));
        LOGF(_log, LOG_LVL_DEBUG,
             "Created CSS CacheFacade %1%" % facades.back().get());
        return;
    }
    if (_cssTech == "mem") {
        LOGF(_log, LOG_LVL_DEBUG, "Creating CSS MemFacade with %1%" % _cssConn);
        facades.push_back(
            css::FacadeFactory::createMemFacade(_cssConn, _emptyChunkPath));
        LOGF(_log, LOG_LVL_DEBUG,
             "Created CSS MemFacade %1%" % facades.back().get());
    } else {
        const std::string errMsg("Invalid CSS technology, check config file.");
        LOGF(_log, LOG_LVL_ERROR, errMsg);
        throw ConfigError(errMsg);
    }
}

void
UserQueryFactory::Impl::purgeFacades() {
    if (facades.size() < 2) {
        return;
    }
    for(auto itr = facades.begin(); itr != facades.end()-1;) {
        if(itr->use_count() == 1) {
            LOGF(_log, LOG_LVL_DEBUG, "Erasing CSS Facade %1%" % itr->get());
            itr = facades.erase(itr);
        } else {
            LOGF(_log, LOG_LVL_DEBUG,
                 "Can't purge CSS Facade%1% (in use)" % itr->get());
            ++itr;
        }
    }
}

}}} // lsst::qserv::ccontrol
