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
/**
  * @file
  *
  * @brief SWIG-exported interface to dispatching queries.
  * Basic usage:
  *
  * newSession() // Init a new session
  *
  * setupQuery(int session, std::string const& query) // setup the session with
  * a query. This triggers a parse.
  *
  * getSessionError(int session) // See if there are errors
  *
  * getConstraints(int session)  // Retrieve the detected constraints so that we
  * can apply them to see which chunks we need. (done in Python)
  *
  * addChunk(int session, lsst::qserv::qproc::ChunkSpec const& cs ) // add the
  * computed chunks to the query
  *
  * submitQuery3(int session) // Trigger the dispatch of all chunk queries for
  * the session.
  *
  * @author Daniel L. Wang, SLAC
  */
#include "ccontrol/UserQuery.h"

// Qserv headers
#include "ccontrol/TmpTableName.h"
#include "ccontrol/ResultReceiver.h"
#include "log/Logger.h"
#include "proto/worker.pb.h"
#include "proto/ProtoImporter.h"
#include "qdisp/Executive.h"
#include "qdisp/MessageStore.h"
#include "qproc/QuerySession.h"
#include "qproc/TaskMsgFactory2.h"
#include "rproc/TableMerger.h"
#include "util/Callable.h"

namespace lsst {
namespace qserv {
qdisp::Executive::Ptr pointer;

class NotifyExecutive : public util::UnaryCallable<void, bool> {
public:
    typedef boost::shared_ptr<NotifyExecutive> Ptr;

    NotifyExecutive(qdisp::Executive::Ptr e, int refNum)
        : _executive(e), _refNum(refNum) {}

    virtual void operator()(bool success) {
        _executive->remove(_refNum);
    }

    static Ptr newInstance(qdisp::Executive::Ptr e, int refNum) {
        return Ptr(new NotifyExecutive(e, refNum));
    }
private:
    qdisp::Executive::Ptr _executive;
    int _refNum;
};

class ProtoPrinter : public util::UnaryCallable<void, boost::shared_ptr<proto::TaskMsg> > {
public:
    ProtoPrinter() {}
    virtual void operator()(boost::shared_ptr<proto::TaskMsg> m) {
        std::cout << "Got taskmsg ok";
    }

};

////////////////////////////////////////////////////////////////////////
namespace ccontrol {

std::string const& UserQuery::getError() const {
    return _qSession->getError();
}

// Consider exposing querySession to userQueryProxy

lsst::qserv::query::ConstraintVec UserQuery::getConstraints() const {
    return _qSession->getConstraints();
}
std::string const& UserQuery::getDominantDb() const {
    return _qSession->getDominantDb();
}

lsst::qserv::css::StripingParams UserQuery::getDbStriping() const {
    return _qSession->getDbStriping();
}

void UserQuery::abort() {
    _executive->abort();
}

void UserQuery::addChunk(qproc::ChunkSpec const& cs) {
    _qSession->addChunk(cs);
}

void UserQuery::submit() {
    _qSession->finalize();
    _setupMerger();
    // Using the QuerySession, generate query specs (text, db, chunkId) and then
    // create query messages and send them to the async query manager.
    qproc::TaskMsgFactory2 f(_sessionId);
    TmpTableName ttn(_sessionId, _qSession->getOriginal());
    std::ostringstream ss;
    proto::ProtoImporter<proto::TaskMsg> pi;
    int msgCount = 0;

    assert(_merger);
    qproc::QuerySession::Iter i;
    qproc::QuerySession::Iter e = _qSession->cQueryEnd();
    // Writing query for each chunk
    for(i = _qSession->cQueryBegin(); i != e; ++i) {
        qproc::ChunkQuerySpec& cs = *i;
        std::string chunkResultName = ttn.make(cs.chunkId);
        ++msgCount;
        f.serializeMsg(cs, chunkResultName, ss);
        std::string msg = ss.str();

        pi(msg.data(), msg.size());
        if(pi.numAccepted() != msgCount) {
            throw "Error serializing TaskMsg.";
        }
#if 1
        ResourceUnit ru;
        ru.setAsDbChunk(cs.db, cs.chunkId);
        boost::shared_ptr<ResultReceiver> rr;
        rr.reset(new ResultReceiver(_merger, chunkResultName));
        int refNum = ++_sequence;
        rr->addFinishHook(NotifyExecutive::newInstance(_executive, refNum));
        qdisp::Executive::Spec s = { ru,
                                     ss.str(),
                                     rr };
        _executive->add(refNum, s);
#else
        TransactionSpec t;
        obsolete::QservPath qp;
        qp.setAsCquery(cs.db, cs.chunkId);
        std::string path=qp.path();
        t.chunkId = cs.chunkId;
        t.msg = ss.str();
        LOGGER_INF << "Msg cid=" << cs.chunkId << " with size="
                   << t.query.size() << std::endl;
        t.bufferSize = 8192000;
        t.path = util::makeUrl(hp.c_str(), qp.path());
        t.savePath = makeSavePath(qm.getScratchPath(), session, cs.chunkId);
        qm.add(t, chunkResultName);
#endif
        ss.str(""); // reset stream

    }
}
QueryState UserQuery::join() {
#if 0
    AsyncQueryManager& qm = getAsyncManager(session);
    qm.joinEverything();
    AsyncQueryManager::ResultDeque const& d = qm.getFinalState();
    bool successful;
    std::for_each(d.begin(), d.end(), mergeStatus(successful));

#endif
    bool successful = _executive->join();
    if(successful) {
        _merger->finalize();
        LOGGER_INF << "Joined everything (success)" << std::endl;
        return SUCCESS;
    } else {
        LOGGER_ERR << "Joined everything (failure!)" << std::endl;
        return ERROR;
    }
}

void UserQuery::discard() {
    // FIXME FIXME!!!
    LOGGER_INF << "Unimplemented UserQuery::discard(). Please implement."
               << std::endl;
}

bool UserQuery::containsDb(std::string const& dbName) const {
    return _qSession->containsDb(dbName);
}

UserQuery::UserQuery(boost::shared_ptr<qproc::QuerySession> qs)
    :  _messageStore(new qdisp::MessageStore()),
       _qSession(qs), _sequence(0) {
    // Some configuration done by factory: See UserQueryFactory
    LOGGER_INF << "Unfinished UserQuery::UserQuery(...). Have a look."
               << std::endl;
    // FIXME FIXME!!!
}

std::string UserQuery::getExecDesc() const {
    return _executive->getProgressDesc();
}

void UserQuery::_setupMerger() {
    // FIXME: would like to re-do plumbing so TableMerger uses
    // mergeStmt more directly
    _mergerConfig->mFixup = _qSession->makeMergeFixup();
    _merger = boost::make_shared<rproc::TableMerger>(*_mergerConfig);
    // Can we configure the merger without involving settings
    // from the python layer? Historically, the Python layer was
    // needed to generate the merging SQL statements, but we are now
    // creating them without Python.
}

}}} // lsst::qserv::ccontrol
