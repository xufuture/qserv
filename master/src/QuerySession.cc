/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
// QuerySession.cc houses the implementation of the class
// QuerySession, which is a container for input query state (and
// related state available prior to execution.
#include "lsst/qserv/master/QuerySession.h"
#include "lsst/qserv/master/SelectParser.h"
namespace qMaster=lsst::qserv::master;
using lsst::qserv::master::QuerySession;

namespace { // File-scope helpers
void build(qMaster::SelectParser::Ptr p) {
    // Perform parse

    // Extract characteristics

    // Need hints to be sent back up to python for scope calculation.
    // hints in sequence of pairs:
    // constraint name --> paramstr (comma separated values)
    // sequences will be passed back up and be re-split. 

    // Prepare templates (2-phase --> 2 templates)

}

} // anonymous namespace

QuerySession::QuerySession() {}

void QuerySession::setQuery(std::string const& q) {
    SelectParser::Ptr p;
    p = SelectParser::newInstance(q);
    p->setup();
    // Perform parse.
    //testParse2(p);    
    // Set error on parse problem; Query manager will check.
}

bool QuerySession::getHasAggregate() const {
    // Aggregate: having an aggregate fct spec in the select list.
    // Stmt itself knows whether aggregation is present. More
    // generally, aggregation is a separate pass. In computing a
    // multi-pass execution, the statement makes use of a (proper,
    // probably) subset of its components to compose each pass. Right
    // now, the only goal is to support aggregation using two passes.
    
    // FIXME
    return false;
}
qMaster::ConstraintVector QuerySession::getConstraints() const {
    // FIXME
    return ConstraintVector();
}

void QuerySession::addChunk(qMaster::ChunkSpec const& cs) {
    // FIXME
}
