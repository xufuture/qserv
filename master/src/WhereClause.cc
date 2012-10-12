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
// WhereClause.cc houses the implementation of a WhereClause, a
// container for the parsed elements of a SQL WHERE.
#include "lsst/qserv/master/WhereClause.h"

#include <iostream>
#include "lsst/qserv/master/QueryTemplate.h"

namespace qMaster=lsst::qserv::master;
using qMaster::QsRestrictor;

namespace { // File-scope helpers
}


////////////////////////////////////////////////////////////////////////
// QsRestirctor::render
////////////////////////////////////////////////////////////////////////
void QsRestrictor::render::operator()(QsRestrictor::Ptr const& p) {
    if(p.get()) {
        qt.append(p->_name);
        qt.append("(");
        StringList::const_iterator i;
        int c=0;
        for(i=p->_params.begin(); i != p->_params.end(); ++i) {
            if(++c > 1) qt.append(",");
            qt.append(*i);
        }
        qt.append(")");
    }
}


////////////////////////////////////////////////////////////////////////
// WhereClause
////////////////////////////////////////////////////////////////////////
std::ostream& 
qMaster::operator<<(std::ostream& os, qMaster::WhereClause const& wc) {
    os << "WHERE " << wc._original;
    return os;
}

std::string
qMaster::WhereClause::getGenerated() {
    QueryTemplate qt;
    
    if(_restrs.get()) {
        std::for_each(_restrs->begin(), _restrs->end(), 
                      QsRestrictor::render(qt));
    }
    return qt.dbgStr();
}

////////////////////////////////////////////////////////////////////////
// WhereClause (private)
////////////////////////////////////////////////////////////////////////
void
qMaster::WhereClause::_resetRestrs() {
    _restrs.reset(new QsRestrictor::List());
}

////////////////////////////////////////////////////////////////////////
// BoolTerm section
////////////////////////////////////////////////////////////////////////
std::ostream& qMaster::OrTerm::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& qMaster::AndTerm::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& qMaster::BoolFactor::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& qMaster::UnknownTerm::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
