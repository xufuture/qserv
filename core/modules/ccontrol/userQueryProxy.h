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

#ifndef LSST_QSERV_CCONTROL_USERQUERYPROXY_H
#define LSST_QSERV_CCONTROL_USERQUERYPROXY_H
/**
  * @file
  *
  * @brief Object-less interface to UserQuery objects, for SWIG-exporting
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <string>

// Local headers
/* #include "control/transaction.h" */
#include "ccontrol/QueryState.h"
#include "css/StripingParams.h"
/* #include "merger/TableMerger.h" */
#include "qproc/ChunkSpec.h"
#include "query/Constraint.h"
/* #include "util/common.h" */
/* #include "xrdc/xrdfile.h" */

namespace lsst {
namespace qserv {

namespace ccontrol {
class UserQuery;

/// @return error description
std::string const& UserQuery_getError(int session);

/// @return discovered constraints in the query
lsst::qserv::query::ConstraintVec UserQuery_getConstraints(int session);

/// @return the dominant db for the query
std::string const& UserQuery_getDominantDb(int session);

/// @return number of stripes and substripes
lsst::qserv::css::StripingParams UserQuery_getDbStriping(int session);

/// Add a chunk spec for execution
void UserQuery_addChunk(int session, qproc::ChunkSpec const& cs );

/// Dispatch all chunk queries for this query
void UserQuery_submit(int session);

QueryState UserQuery_join(int session);
std::string const& UserQuery_getQueryStateString(QueryState const& qs);
//std::string UserQuery_getErrorDesc(int session);

void UserQuery_discard(int session);

/// @return sessionId
int UserQuery_takeOwnership(UserQuery* uq);

}}} // namespace lsst::qserv:ccontrol

#endif // LSST_QSERV_CCONTROL_USERQUERYPROXY_H
