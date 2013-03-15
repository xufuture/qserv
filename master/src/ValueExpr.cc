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
#include "lsst/qserv/master/ValueExpr.h"
#include <iostream>
#include <sstream>
#include <iterator>
#include "lsst/qserv/master/ColumnRef.h"
#include "lsst/qserv/master/QueryTemplate.h"
#include "lsst/qserv/master/FuncExpr.h"

namespace qMaster=lsst::qserv::master;
using lsst::qserv::master::ValueExpr;
using lsst::qserv::master::ValueExprPtr;

std::ostream& 
qMaster::output(std::ostream& os, qMaster::ValueExprList const& vel) {
    std::copy(vel.begin(), vel.end(),
              std::ostream_iterator<qMaster::ValueExprPtr>(os, ";"));    
    return os;
}
void 
qMaster::renderList(qMaster::QueryTemplate& qt, 
                    qMaster::ValueExprList const& vel) {
    std::for_each(vel.begin(), vel.end(), ValueExpr::render(qt));
}

namespace { // File-scope helpers

}

std::ostream& qMaster::operator<<(std::ostream& os, qMaster::ColumnRef const& cr) {
    return os << "(" << cr.db << "," << cr.table << "," << cr.column << ")";
}
std::ostream& qMaster::operator<<(std::ostream& os, qMaster::ColumnRef const* cr) {
    return os << *cr;
}
void qMaster::ColumnRef::render(QueryTemplate& qt) const {
#if 0
    if(!db.empty()) { qt.append(db); qt.append("."); }
    if(!table.empty()) {qt.append(table); qt.append("."); }
    qt.append(column);
#endif
    qt.append(*this);
}


ValueExprPtr ValueExpr::newColumnRefExpr(boost::shared_ptr<ColumnRef const> cr) {
    ValueExprPtr expr(new ValueExpr());
    expr->_type = COLUMNREF;
    expr->_columnRef.reset(new ColumnRef(*cr));
    return expr;
}

ValueExprPtr ValueExpr::newStarExpr(std::string const& table) {
    ValueExprPtr expr(new ValueExpr());
    expr->_type = STAR;
    if(!table.empty()) {
        expr->_tableStar = table;
    }
    return expr;
}
ValueExprPtr ValueExpr::newFuncExpr(boost::shared_ptr<FuncExpr> fe) {
    ValueExprPtr expr(new ValueExpr());
    expr->_type = FUNCTION;
    expr->_funcExpr = fe;
    return expr;
}

ValueExprPtr ValueExpr::newAggExpr(boost::shared_ptr<FuncExpr> fe) {
    ValueExprPtr expr(new ValueExpr());
    expr->_type = AGGFUNC;
    expr->_funcExpr = fe;
    return expr;
}

ValueExprPtr
ValueExpr::newConstExpr(std::string const& alnum) {
    ValueExprPtr expr(new ValueExpr());
    expr->_type = CONST;
    expr->_tableStar = alnum;
    return expr;
}

ValueExprPtr ValueExpr::clone() const{
    ValueExprPtr expr(new ValueExpr(*this));
    return expr;
}


std::ostream& qMaster::operator<<(std::ostream& os, ValueExpr const& ve) {
    switch(ve._type) {
    case ValueExpr::COLUMNREF: os << "CREF: " << *(ve._columnRef); break;
    case ValueExpr::FUNCTION: os << "FUNC: " << *(ve._funcExpr); break;
    case ValueExpr::AGGFUNC: os << "AGGFUNC: " << *(ve._funcExpr); break;
    case ValueExpr::STAR:
        os << "<";
        if(!ve._tableStar.empty()) os << ve._tableStar << ".";
        os << "*>"; 
        break;
    case ValueExpr::CONST: 
        os << "CONST: " << ve._tableStar;
        break;
    default: os << "UnknownExpr"; break;
    }
    if(!ve._alias.empty()) { os << " [" << ve._alias << "]"; }
    return os;
}

std::ostream& qMaster::operator<<(std::ostream& os, ValueExpr const* ve) {
    if(!ve) return os << "<NULL>";
    return os << *ve;
}

void qMaster::ValueExpr::render::operator()(qMaster::ValueExpr const& ve) {
    std::stringstream ss;
    if(_count++ > 0) _qt.append(",");
    switch(ve._type) {
    case ValueExpr::COLUMNREF: ve._columnRef->render(_qt); break;
    case ValueExpr::FUNCTION: ve._funcExpr->render(_qt); break;
    case ValueExpr::AGGFUNC: ve._funcExpr->render(_qt); break;
    case ValueExpr::STAR: 
        if(!ve._tableStar.empty()) {
            _qt.append(ColumnRef("",ve._tableStar, "*"));
        } else {
            _qt.append("*");
        }
        break;
    case ValueExpr::CONST: _qt.append(ve._tableStar); break;
    default: break;
    }
    if(!ve._alias.empty()) { _qt.append("AS"); _qt.append(ve._alias); }
}

