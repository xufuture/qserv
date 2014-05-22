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
  * Basic usage:
  *
  * Construct a UserQueryFactory, then create a new UserQuery
  * object. You will get a session ID that will identify the UserQuery
  * for use with this proxy. The query is parsed and prepared for
  * execution as much as possible, without knowing partition coverage.
  *
  * 
  * UserQuery_getError(int session) // See if there are errors
  *
  * UserQuery_getConstraints(int session)  // Retrieve the detected
  * constraints so that we can apply them to see which chunks we
  * need. (done in Python) 
  *
  * UserQuery_addChunk(int session, ChunkSpec const& cs ) 
  * // add the computed chunks to the query
  *
  * UserQuery_submit(int session) // Trigger the dispatch of all chunk
  * queries for the UserQuery
  *
  * @author Daniel L. Wang, SLAC
  */

#include "ccontrol/SessionManager.h"
#include "ccontrol/userQueryProxy.h"

// System headers
//#include <fstream>
//#include <sstream>

// Local headers
#include "ccontrol/MissingUserQuery.h"
#include "ccontrol/UserQuery.h"
#include "log/Logger.h"
#include "util/StringHash.h"

namespace {
std::string makeSavePath(std::string const& dir,
                         int sessionId,
                         int chunkId,
                         unsigned seq=0) {
    std::stringstream ss;
    ss << dir << "/" << sessionId << "_" << chunkId << "_" << seq;
    return ss.str();
}

class TmpTableName {
public:
    TmpTableName(int sessionId, std::string const& query) {
        std::stringstream ss;
        ss << "r_" << sessionId
           << lsst::qserv::util::StringHash::getMd5Hex(query.data(), query.size())
           << "_";
        _prefix = ss.str();
    }
    std::string make(int chunkId, int seq=0) {
        std::stringstream ss;
        ss << _prefix << chunkId << "_" << seq;
        return ss.str();
    }
private:
    std::string _prefix;
};

} // anonymous namespace


namespace lsst {
namespace qserv {
namespace ccontrol {

class UserQueryManager : public SessionManager<UserQuery::Ptr> {
public:
    UserQueryManager() {}
    UserQuery::Ptr get(int id) {
        UserQuery::Ptr p = getSession(id);
        if(!p) {
            throw MissingUserQuery(id);
        }
        return p;
    }
};
static UserQueryManager uqManager;

std::string const& UserQuery_getError(int session) {
    return uqManager.get(session)->getError();
}

/// @return discovered constraints in the query
lsst::qserv::query::ConstraintVec UserQuery_getConstraints(int session) {
    return uqManager.get(session)->getConstraints();
}

/// @return the dominant db for the query
std::string const& UserQuery_getDominantDb(int session) {
    return uqManager.get(session)->getDominantDb();
}

/// @return number of stripes and substripes
lsst::qserv::css::StripingParams UserQuery_getDbStriping(int session) {
    return uqManager.get(session)->getDbStriping();
}

/// Add a chunk spec for execution
void UserQuery_addChunk(int session, qproc::ChunkSpec const& cs) {
    uqManager.get(session)->addChunk(cs);
}

/// Dispatch all chunk queries for this query
void UserQuery_submit(int session) {
    LOGGER_DBG << "EXECUTING submitQuery3(" << session << ")" << std::endl;
    uqManager.get(session)->submit();
}

/// Block until execution succeeds or fails completely
QueryState UserQuery_join(int session) {
    return uqManager.get(session)->join();
}

/// Discard the UserQuery by destroying it and forgetting about its id.
void UserQuery_discard(int session) {
    UserQuery::Ptr p = uqManager.get(session);
    p->discard();
    p.reset();
    uqManager.discardSession(session);
}
}}} // namespace lsst::qserv::ccontrol
