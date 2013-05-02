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
// ValueExpr.cc houses the implementation of ValueExpr, a object
// containing elements of a SQL value expresssion (construct that
// evaluates to a [non-boolean] SQL primitive value)
// ValueExpr elements are formed as 'term (op term)*' .
#include "lsst/qserv/master/ValueExpr.h"
#include <iostream>
#include <sstream>
#include <iterator>
#include "lsst/qserv/master/ValueTerm.h"
#include "lsst/qserv/master/QueryTemplate.h"
#include "lsst/qserv/master/FuncExpr.h"

namespace qMaster=lsst::qserv::master;


namespace lsst { namespace qserv { namespace master {
std::ostream& 
output(std::ostream& os, ValueExprList const& vel) {
    std::copy(vel.begin(), vel.end(),
              std::ostream_iterator<qMaster::ValueExprPtr>(os, ";"));    
    return os;
}
void 
renderList(QueryTemplate& qt, ValueExprList const& vel) {
    std::for_each(vel.begin(), vel.end(), ValueExpr::render(qt, true));
}

////////////////////////////////////////////////////////////////////////
// ValueExpr::
////////////////////////////////////////////////////////////////////////

class trivialCopy {
public:
    inline ValueTermPtr operator()(ValueTerm const& t) {
        return ValueTermPtr(new ValueTerm(t));
    }
};

////////////////////////////////////////////////////////////////////////
// ValueExpr statics
////////////////////////////////////////////////////////////////////////
ValueExprPtr ValueExpr::newSimple(boost::shared_ptr<ValueTerm> vt)  {
    assert(vt.get());
    boost::shared_ptr<ValueExpr> ve(new ValueExpr);
    TermOp t = {vt, NONE};
    ve->_termOps.push_back(t);
    return ve;
}
////////////////////////////////////////////////////////////////////////
// ValueExpr 
////////////////////////////////////////////////////////////////////////
ValueExpr::ValueExpr() {
}

ValueExprPtr ValueExpr::clone() const {
#if 0
    ValueExprPtr expr(new ValueExpr);
    if(_term.get()) { expr->_term.reset(new ValueTerm(*_term)); }
    expr->_op = _op;
    // FIXME: Convert tail-recursion to iterative
    if(_next.get()) { expr->_next = _next->clone(); }
    return expr;
#elif 0
    copyTransformExpr copyF(trivialCopy());
    mapTerms(*this, copyF);
    return copyF.root;
#else
    // First, make a shallow copy
    ValueExprPtr expr(new ValueExpr(*this)); 
    TermOpList::iterator ti = expr->_termOps.begin();
    for(TermOpList::const_iterator i=_termOps.begin();
        i != _termOps.end(); ++i, ++ti) {
        // Deep copy (clone) each term.
        ti->term = i->term->clone();
    }
    return expr;
#endif
}


std::ostream& operator<<(std::ostream& os, ValueExpr const& ve) {
    // FIXME: Currently only considers first term.
    if(!ve._termOps.empty()) {
        os << *(ve._termOps.front().term);
    } else {
        os << "EmptyValueExpr";
    }
    if(!ve._alias.empty()) { os << " [" << ve._alias << "]"; }
    return os;
}

std::ostream& operator<<(std::ostream& os, ValueExpr const* ve) {
    if(!ve) return os << "<NULL>";
    return os << *ve;
}
////////////////////////////////////////////////////////////////////////
// ValueExpr::render
////////////////////////////////////////////////////////////////////////
void ValueExpr::render::operator()(qMaster::ValueExpr const& ve) {
    if(_needsComma && _count++ > 0) { _qt.append(","); }
    ValueTerm::render render(_qt);
    for(TermOpList::const_iterator i=ve._termOps.begin();
        i != ve._termOps.end(); ++i) {
        render(i->term);
        switch(i->op) {
        case ValueExpr::NONE: break;
        case ValueExpr::UNKNOWN: _qt.append("<UNKNOWN_OP>"); break;
        case ValueExpr::PLUS: _qt.append("+"); break;
        case ValueExpr::MINUS: _qt.append("-"); break;
        default:
            std::ostringstream ss;
            ss << "Corruption: bad _op in ValueExpr optype=" << i->op;
            // FIXME: Make sure this never happens.
            throw ss.str();
        }
    }
    if(!ve._alias.empty()) { _qt.append("AS"); _qt.append(ve._alias); }
}

}}} // lsst::qserv::master
