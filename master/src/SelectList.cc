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

// Idea was to have this as an intermediate query tree representation.
// ANTLR-specific manipulation crept into this container due to
// convenience, but let's think of a way to isolate the ANTLR portion
// without being too complicated.
#include "lsst/qserv/master/SelectList.h"
#include <iterator>

#include "SqlSQL2TokenTypes.hpp" // For ANTLR typing.

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
SelectList::addFunc(antlr::RefAST a) {
    assert(_valueExprList.get());
   boost::shared_ptr<FuncExpr> fe(new FuncExpr());
   if(a->getType() == SqlSQL2TokenTypes::FUNCTION_SPEC) { a = a->getFirstChild(); }
   std::cout << "fspec name:" << tokenText(a) << std::endl;
   fe->name = tokenText(a);
   _fillParams(fe->params, a->getNextSibling());
   _valueExprList->push_back(ValueExpr::newFuncExpr(fe));
}

void
SelectList::addAgg(antlr::RefAST a) {
    assert(_valueExprList.get());
   boost::shared_ptr<FuncExpr> fe(new FuncExpr());
   fe->name = tokenText(a);
   _fillParams(fe->params, a->getFirstChild());
   _valueExprList->push_back(ValueExpr::newAggExpr(fe));
}

void
SelectList::addRegular(antlr::RefAST a) {
    assert(_valueExprList.get());
    // boost::shared_ptr<FuncExpr> fe(new Expr());
   // ValueExprPtr ValueExpr::newColumnRefExpr(boost::shared_ptr<ColumnRef> cr) {

   // fe->name = tokenText(a);
   // _fillParams(fe->params, a->getFirstChild());
    std::cout << "FIXME: addRegular confounded with column ref handler now." << std::endl;
    //_valueExprList->push_back(ValueExpr::newColumnRefExpr(a));
}

void
SelectList::dbgPrint() const {
    assert(_valueExprList.get());
    std::copy(_valueExprList->begin(),
              _valueExprList->end(),
              std::ostream_iterator<ValueExprPtr>(std::cout, "\n"));
    
}

ValueExprPtr _newValueExpr(antlr::RefAST v) {
    ValueExprPtr e(new ValueExpr());
    // Figure out what type of value expr, and create it properly.
    std::cout << "Type of:" << v->getText() << "(" << v->getType() << ")" << std::endl;
    return e;
}

void 
SelectList::_fillParams(ValueExprList& p, antlr::RefAST pnodes) {
    antlr::RefAST current = pnodes;
    std::cout << "params got " << tokenText(pnodes) << std::endl;
    // Make sure the parser gave us the right nodes.
    assert(current->getType() == SqlSQL2TokenTypes::LEFT_PAREN); 
    for(current = current->getNextSibling(); 
        current.get(); 
        current=current->getNextSibling()) {
        if(current->getType() == SqlSQL2TokenTypes::COMMA) { continue; }
        if(current->getType() == SqlSQL2TokenTypes::RIGHT_PAREN) { break; }
        p.push_back(_newValueExpr(current));
    }
    std::cout << "printing params \n";
    printIndented(pnodes);
    // FIXME
}
