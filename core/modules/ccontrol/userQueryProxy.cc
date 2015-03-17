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
/**
  * @file
  *
  * Basic usage:
  *
  * Construct a UserQueryFactory, then create a new UserQuery
  * object. You will get a session ID that will identify the UserQuery
  * for use with this proxy. The query is parsed and prepared for
  * execution as much as possible, without knowing partition coverage.
  *
  * UserQuery_getError(int session) // See if there are errors
  *
  * UserQuery_submit(int session) // Trigger the dispatch of all chunk
  * queries for the UserQuery
  *
  * @author Daniel L. Wang, SLAC
  */

// Class header
#include "ccontrol/userQueryProxy.h"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "ccontrol/UserQuery.h"
#include "qdisp/MessageStore.h"

namespace lsst {
namespace qserv {
namespace ccontrol {

/// @return a string describing the error state of the query
std::string UserQuery_getError(int session) {
    return UserQuery::get(session)->getError();
}

/// @return a string describing the progress on the query at a chunk-by-chunk
/// level.  Useful for diagnosis when queries are squashed or return errors.
std::string UserQuery_getExecDesc(int session) {
    return UserQuery::get(session)->getExecDesc();
}

/// Abort a running query
void UserQuery_kill(int session) {
    LOGF_INFO("EXECUTING UserQuery_kill(%1%)" % session);
    UserQuery::get(session)->kill();
}

/// Add a chunk spec for execution
void UserQuery_addChunk(int session, qproc::ChunkSpec const& cs) {
    UserQuery::get(session)->addChunk(cs);
}

/// Dispatch all chunk queries for this query
void UserQuery_submit(int session) {
    LOGF_DEBUG("EXECUTING UserQuery_submit(%1%)" % session);
    UserQuery::get(session)->submit();
}

/// Block until execution succeeds or fails completely
/// @return a QueryState constant indicating SUCCESS or ERROR
QueryState UserQuery_join(int session) {
    return UserQuery::get(session)->join();
}

/// Discard the UserQuery by destroying it and forgetting about its id
void UserQuery_discard(int session) {
    UserQuery::get(session)->discard();
}

/// @return count of messages in UserQuery's message store
int UserQuery_getMsgCount(int session) {
    return UserQuery::get(session)->getMessageStore()->messageCount();
}

/// @return message info from the UserQuery's message store
/// Python call syntax: message, chunkId, code, timestamp = UserQuery_getMesg(session, idx)
std::string UserQuery_getMsg(int session, int idx, int* chunkId, int* code, time_t* timestamp) {
    qdisp::QueryMessage msg = UserQuery::get(session)->getMessageStore()->getMessage(idx);
    *chunkId = msg.chunkId;
    *code = msg.code;
    *timestamp = msg.timestamp;
    return msg.description;
}

/// Add a message to the UserQuery's message store
void UserQuery_addMsg(int session, int chunkId, int code, std::string const& message) {
    UserQuery::get(session)->getMessageStore()->addMessage(chunkId, code, message);
}

}}} // namespace lsst::qserv::ccontrol
