/*
 * LSST Data Management System
 * Copyright 2013-2014 LSST Corporation.
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
  * @file TableRefN.cc
  *
  * @brief TableRefN, SimpleTableN, JoinRefN implementations
  *
  * @author Daniel L. Wang, SLAC
  */
#include "query/TableRef.h"
#include <sstream>
#include "query/JoinRef.h"
#include "query/JoinSpec.h"

namespace qMaster=lsst::qserv::master;

namespace lsst {
namespace qserv {
namespace master {
////////////////////////////////////////////////////////////////////////
// TableRef
////////////////////////////////////////////////////////////////////////
std::ostream& operator<<(std::ostream& os, TableRef const& ref) {
    return ref.putStream(os);
}
std::ostream& operator<<(std::ostream& os, TableRef const* ref) {
    return ref->putStream(os);
}

void TableRef::render::operator()(TableRef const& ref) {
    if(_count++ > 0) _qt.append(",");
    ref.putTemplate(_qt);
}

std::ostream& TableRef::putStream(std::ostream& os) const {
    os << "Table(" << db << "." << table << ")";
    if(!alias.empty()) { os << " AS " << alias; }
    typedef JoinRefList::const_iterator Iter;
    for(Iter i=joinRefList.begin(), e=joinRefList.end(); i != e; ++i) {
        JoinRef const& j = **i;
        os << " " << j;
    }
    return os;
}

void TableRef::putTemplate(QueryTemplate& qt) const {
    if(!db.empty()) {
        qt.append(db); // Use TableEntry?
        qt.append(".");
    }
    qt.append(table);
    if(!alias.empty()) {
        qt.append("AS");
        qt.append(alias);
    }
    typedef JoinRefList::const_iterator Iter;
    for(Iter i=joinRefList.begin(), e=joinRefList.end(); i != e; ++i) {
        JoinRef const& j = **i;
        j.putTemplate(qt);
    }
}

void TableRef::addJoin(boost::shared_ptr<JoinRef> r) {
    joinRefList.push_back(r);
}

void TableRef::applySimple(TableRef::Func& f) {
    f(*this);
    typedef JoinRefList::iterator Iter;
    for(Iter i=joinRefList.begin(), e=joinRefList.end(); i != e; ++i) {
        JoinRef& j = **i;
        j.getRight()->applySimple(f);
    }
}

std::list<TableRef::Ptr> TableRef::permute(TableRef::Pfunc& p) const {
    throw std::logic_error("Undefined");
}
}}} // lsst::qserv::master
