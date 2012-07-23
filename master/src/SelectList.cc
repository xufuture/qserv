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
// SelectList
 
#include "lsst/qserv/master/SelectList.h"
#include <iterator>

using lsst::qserv::master::ColumnRef;
using lsst::qserv::master::ColumnRefList;
using lsst::qserv::master::ValueExpr;
using lsst::qserv::master::ValueExprPtr;
using lsst::qserv::master::SelectList;
namespace qMaster=lsst::qserv::master;

namespace { // File-scope helpers
} // anonymous namespace

ValueExprPtr ValueExpr::newColumnRefExpr(boost::shared_ptr<ColumnRef> cr) {
    ValueExprPtr expr(new ValueExpr());
    expr->_type = COLUMNREF;
    expr->_columnRef = cr;
    return expr;
}

ValueExprPtr ValueExpr::newStarExpr() {
    ValueExprPtr expr(new ValueExpr());
    expr->_type = STAR;
    return expr;
}

std::ostream& qMaster::operator<<(std::ostream& os, ValueExpr const& ve) {
    switch(ve._type) {
    case ValueExpr::COLUMNREF: os << "cref"; break;
    case ValueExpr::FUNCTION: os << "Func"; break;
    case ValueExpr::STAR: os << "*"; break;
    default: os << "UnknownExpr"; break;
    }
    return os;
}
std::ostream& qMaster::operator<<(std::ostream& os, ValueExpr const* ve) {
    return os << *ve;
}

void
ColumnRefList::acceptColumnRef(antlr::RefAST d, antlr::RefAST t, 
                               antlr::RefAST c) {
    using lsst::qserv::master::tokenText;
    boost::shared_ptr<ColumnRef> cr(new ColumnRef(tokenText(d), 
                                                  tokenText(t), 
                                                  tokenText(c)));
    _refs.push_back(*cr);
    if(_valueExprList.get()) {
        _valueExprList->push_back(ValueExpr::newColumnRefExpr(cr));
    }
}

void
SelectList::addStar(antlr::RefAST a) {
    assert(_valueExprList.get());
    _valueExprList->push_back(ValueExpr::newStarExpr());
}

void
SelectList::dbgPrint() const {
    assert(_valueExprList.get());
    std::copy(_valueExprList->begin(),
              _valueExprList->end(),
              std::ostream_iterator<ValueExprPtr>(std::cout, "\n"));
    
}
