/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
// OrderByClause, OrderByTerm, HavingClause implementations
#include "lsst/qserv/master/OrderByClause.h"
#include <iostream>
#include <iterator>
#include <sstream>

#include <boost/make_shared.hpp>
#include "lsst/qserv/master/QueryTemplate.h"

using lsst::qserv::master::OrderByClause;
using lsst::qserv::master::HavingClause;
namespace qMaster=lsst::qserv::master;

namespace { // File-scope helpers

} // anonymous namespace


////////////////////////////////////////////////////////////////////////
// OrderByTerm
////////////////////////////////////////////////////////////////////////
std::ostream& 
qMaster::operator<<(std::ostream& os, qMaster::OrderByTerm const& t) {
    os << *(t._expr);
    if(!t._collate.empty()) os << " COLLATE " << t._collate;
    switch(t._order) {
    case qMaster::OrderByTerm::ASC: os << " ASC"; break;
    case qMaster::OrderByTerm::DESC: os << " DESC"; break;
    case qMaster::OrderByTerm::DEFAULT: break;
    }
    return os;
}
////////////////////////////////////////////////////////////////////////
// OrderByClause
////////////////////////////////////////////////////////////////////////
std::ostream& 
qMaster::operator<<(std::ostream& os, qMaster::OrderByClause const& c) {
    if(c._terms.get()) {
        os << "ORDER BY ";
        std::copy(c._terms->begin(),c._terms->end(),
                  std::ostream_iterator<qMaster::OrderByTerm>(os,", "));
    }
    return os;
}

std::string
qMaster::OrderByClause::getGenerated() {
    QueryTemplate qt;
    renderTo(qt);
    return qt.dbgStr();
}

void
qMaster::OrderByClause::renderTo(qMaster::QueryTemplate& qt) const {
    std::stringstream ss;
    if(_terms.get()) {
        std::copy(_terms->begin(), _terms->end(),
                  std::ostream_iterator<qMaster::OrderByTerm>(ss,", "));
    }
    qt.append(ss.str());
    // FIXME
}
boost::shared_ptr<OrderByClause> OrderByClause::copyDeep() {
    return boost::make_shared<OrderByClause>(*this); // FIXME
}
boost::shared_ptr<OrderByClause> OrderByClause::copySyntax() {
    return boost::make_shared<OrderByClause>(*this);
}
////////////////////////////////////////////////////////////////////////
// HavingClause
////////////////////////////////////////////////////////////////////////
std::ostream& 
qMaster::operator<<(std::ostream& os, qMaster::HavingClause const& c) {
    if(!c._expr.empty()) {
        os << "HAVING " << c._expr;
    }
    return os;
}
std::string
qMaster::HavingClause::getGenerated() {
    QueryTemplate qt;
    renderTo(qt);
    return qt.dbgStr();
}
void
qMaster::HavingClause::renderTo(qMaster::QueryTemplate& qt) const {
    if(!_expr.empty()) {
        qt.append(_expr);
    }
}

boost::shared_ptr<HavingClause> HavingClause::copyDeep() {
    return boost::make_shared<HavingClause>(*this); // FIXME
}
boost::shared_ptr<HavingClause> HavingClause::copySyntax() {
    return boost::make_shared<HavingClause>(*this);
}
