/* 
 * LSST Data Management System
 * Copyright 2012-2013 LSST Corporation.
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
#include "lsst/qserv/master/ValueTerm.h"
#include <iostream>
#include <sstream>
#include <iterator>
#include "lsst/qserv/master/ColumnRef.h"
#include "lsst/qserv/master/QueryTemplate.h"
#include "lsst/qserv/master/FuncExpr.h"

namespace qMaster=lsst::qserv::master;
using lsst::qserv::master::ValueTerm;
using lsst::qserv::master::ValueTermPtr;

namespace lsst { namespace qserv { namespace master {

ValueTermPtr ValueTerm::newColumnRefTerm(boost::shared_ptr<ColumnRef const> cr) {
    ValueTermPtr term(new ValueTerm());
    term->_type = COLUMNREF;
    term->_columnRef.reset(new ColumnRef(*cr));
    return term;
}

ValueTermPtr ValueTerm::newStarTerm(std::string const& table) {
    ValueTermPtr term(new ValueTerm());
    term->_type = STAR;
    if(!table.empty()) {
        term->_tableStar = table;
    }
    return term;
}
ValueTermPtr ValueTerm::newFuncTerm(boost::shared_ptr<FuncExpr> fe) {
    ValueTermPtr term(new ValueTerm());
    term->_type = FUNCTION;
    term->_funcExpr = fe;
    return term;
}

ValueTermPtr ValueTerm::newAggTerm(boost::shared_ptr<FuncExpr> fe) {
    ValueTermPtr term(new ValueTerm());
    term->_type = AGGFUNC;
    term->_funcExpr = fe;
    return term;
}

ValueTermPtr
ValueTerm::newConstTerm(std::string const& alnum) {
    ValueTermPtr term(new ValueTerm());
    term->_type = CONST;
    term->_tableStar = alnum;
    return term;
}

ValueTermPtr ValueTerm::clone() const{
    ValueTermPtr expr(new ValueTerm(*this));
    // Clone refs.
    if(_columnRef.get()) {
        expr->_columnRef.reset(new ColumnRef(*_columnRef));
    }
    if(_funcExpr.get()) {
        expr->_funcExpr.reset(new FuncExpr(*_funcExpr));
    }
    return expr;
}

std::ostream& operator<<(std::ostream& os, ValueTerm const& ve) {
    switch(ve._type) {
    case ValueTerm::COLUMNREF: os << "CREF: " << *(ve._columnRef); break;
    case ValueTerm::FUNCTION: os << "FUNC: " << *(ve._funcExpr); break;
    case ValueTerm::AGGFUNC: os << "AGGFUNC: " << *(ve._funcExpr); break;
    case ValueTerm::STAR:
        os << "<";
        if(!ve._tableStar.empty()) os << ve._tableStar << ".";
        os << "*>"; 
        break;
    case ValueTerm::CONST: 
        os << "CONST: " << ve._tableStar;
        break;
    default: os << "UnknownTerm"; break;
    }
    if(!ve._alias.empty()) { os << " [" << ve._alias << "]"; }
    return os;
}

std::ostream& operator<<(std::ostream& os, ValueTerm const* ve) {
    if(!ve) return os << "<NULL>";
    return os << *ve;
}

void ValueTerm::render::operator()(qMaster::ValueTerm const& ve) {
    switch(ve._type) {
    case ValueTerm::COLUMNREF: ve._columnRef->render(_qt); break;
    case ValueTerm::FUNCTION: ve._funcExpr->render(_qt); break;
    case ValueTerm::AGGFUNC: ve._funcExpr->render(_qt); break;
    case ValueTerm::STAR: 
        if(!ve._tableStar.empty()) {
            _qt.append(ColumnRef("",ve._tableStar, "*"));
        } else {
            _qt.append("*");
        }
        break;
    case ValueTerm::CONST: _qt.append(ve._tableStar); break;
    default: break;
    }
    if(!ve._alias.empty()) { _qt.append("AS"); _qt.append(ve._alias); }
}

}}} // lsst::qserv::master
