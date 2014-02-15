/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
#include "query/TableRefN.h"
#include <sstream>
#include "query/JoinSpec.h"

namespace qMaster=lsst::qserv::master;

namespace lsst {
namespace qserv {
namespace master {
////////////////////////////////////////////////////////////////////////
// TableRefN
////////////////////////////////////////////////////////////////////////
std::ostream& operator<<(std::ostream& os, TableRefN const& refN) {
    return refN.putStream(os);
}
std::ostream& operator<<(std::ostream& os, TableRefN const* refN) {
    return refN->putStream(os);
}

void TableRefN::render::operator()(TableRefN const& refN) {
    if(_count++ > 0) _qt.append(",");
    refN.putTemplate(_qt);
}
std::list<TableRefN::Ptr> TableRefN::permute(TableRefN::Pfunc& p) const {
    throw std::logic_error("Undefined");
}

////////////////////////////////////////////////////////////////////////
// SimpleTableN
////////////////////////////////////////////////////////////////////////
std::list<TableRefN::Ptr> SimpleTableN::permute(TableRefN::Pfunc& p) const {
    return p(*this);
}

////////////////////////////////////////////////////////////////////////
// JoinRefN
////////////////////////////////////////////////////////////////////////
std::ostream&
JoinRefN::putStream(std::ostream& os) const {
    QueryTemplate jt;
    _putJoinTemplate(jt);

    os << "Join(";
    if(left) { left->putStream(os); }
    else { os << "<BROKEN_JOIN>";}
    os << jt.generate();
    if(right) {right->putStream(os); }
    else { os << "<BROKEN_JOIN>";}
    return os;
}
void JoinRefN::putTemplate(QueryTemplate& qt) const {
    if(!left || !right) {
        throw std::logic_error("Invalid JoinTableN for templating");
    }
    left->putTemplate(qt);
    _putJoinTemplate(qt);
    right->putTemplate(qt);
    if(spec) spec->putTemplate(qt);
}

void JoinRefN::apply(TableRefN::Func& f) {
    if(left) { left->apply(f); }
    if(right ) { right->apply(f); }
}
void JoinRefN::apply(TableRefN::FuncConst& f) const {
    if(left) { left->apply(f); }
    if(right ) { right->apply(f); }
}
std::list<TableRefN::Ptr> JoinRefN::permute(TableRefN::Pfunc& p) const {
    // Apply on both sides of the join statement
    std::list<TableRefN::Ptr> lefts = left->permute(p);
    std::list<TableRefN::Ptr> rights = right->permute(p);
    PtrList pList;
    // Construct a new JoinTableN for each permutation using li and ri,
    // such that li is an element of lefts and ri is an element of rights.
    for(PtrList::iterator i = lefts.begin(); i != lefts.end(); ++i) {
        for(PtrList::iterator j = rights.begin(); j != rights.end(); ++j) {
            Ptr jt(new JoinRefN(*i, *j, joinType, isNatural, spec->clone()));
            pList.push_back(jt);
        }
    }
    return pList;
}

void JoinRefN::_putJoinTemplate(QueryTemplate& qt) const {
    if(isNatural) { qt.append("NATURAL"); }

    switch(joinType) {
    case DEFAULT: break;
    case INNER: qt.append("INNER"); break;
    case LEFT: qt.append("LEFT"); qt.append("OUTER"); break;
    case RIGHT: qt.append("RIGHT"); qt.append("OUTER"); break;
    case FULL: qt.append("FULL"); qt.append("OUTER"); break;
    case CROSS: qt.append("CROSS"); break;
    case UNION: qt.append("INNER"); break;
    }

    qt.append("JOIN");
}

}}} // lsst::qserv::master
