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
// ValueExprFactory.cc constructs ValueExpr instances from an ANTLR
// subtrees.

#include "lsst/qserv/master/ValueExprFactory.h"
#include "lsst/qserv/master/ValueTermFactory.h"
#include "lsst/qserv/master/ColumnRefH.h"
#if 0
#include "lsst/qserv/master/ColumnRef.h"
#include "lsst/qserv/master/FuncExpr.h"
#include "lsst/qserv/master/parseTreeUtil.h"
#endif
#include "lsst/qserv/master/ValueExpr.h" // For ValueExpr, FuncExpr
#include "lsst/qserv/master/ValueTerm.h" // For ValueTerm
#include "SqlSQL2TokenTypes.hpp" 


// namespace modifiers
namespace qMaster = lsst::qserv::master;
using antlr::RefAST;

////////////////////////////////////////////////////////////////////////
// Anonymous helpers
////////////////////////////////////////////////////////////////////////
namespace {

} // anonymous
namespace lsst { namespace qserv { namespace master {
////////////////////////////////////////////////////////////////////////
// ValueExprFactory implementation
////////////////////////////////////////////////////////////////////////
ValueExprFactory::ValueExprFactory(boost::shared_ptr<ColumnRefMap> cMap) 
    : _valueTermFactory(new ValueTermFactory(cMap)) {
}

// VALUE_EXP                     //
// |      \                      //
// TERM   (TERM_OP TERM)*        //
boost::shared_ptr<ValueExpr> 
ValueExprFactory::newExpr(antlr::RefAST a) {
#if 0
    boost::shared_ptr<ValueExpr> root;
    boost::shared_ptr<ValueExpr> parent;
    boost::shared_ptr<ValueExpr> currentVe;
    std::cout << walkIndentedString(a) << std::endl;
    while(a.get()) {
        currentVe.reset(new ValueExpr);
        if(parent.get()) { // attach to parent.
            parent->_next = currentVe;
        } else { // No parent, so set the root.
            root = currentVe;
        }
        currentVe->_term = _valueTermFactory->newTerm(a);
        RefAST op = a->getNextSibling();
        if(!op.get()) { // No more ops?
            break;
        }
        int eType = op->getType();
        switch(op->getType()) {
        case SqlSQL2TokenTypes::PLUS_SIGN:
            currentVe->_op = ValueExpr::PLUS;
            break;
        case SqlSQL2TokenTypes::MINUS_SIGN:
            currentVe->_op = ValueExpr::MINUS;
            break;
        default: 
            currentVe->_op = ValueExpr::UNKNOWN;
            throw std::string("unhandled term_op type.");
        }
        a = op->getNextSibling();
        parent = currentVe;
    }
    return root;
#else
    boost::shared_ptr<ValueExpr> expr(new ValueExpr);
    std::cout << walkIndentedString(a) << std::endl;
    while(a.get()) {
        ValueExpr::TermOp newTermOp;
        newTermOp.term = _valueTermFactory->newTerm(a);
        RefAST op = a->getNextSibling();
        if(!op.get()) { // No more ops?
            break;
        }
        int eType = op->getType();
        switch(op->getType()) {
        case SqlSQL2TokenTypes::PLUS_SIGN:
            newTermOp.op = ValueExpr::PLUS;
            break;
        case SqlSQL2TokenTypes::MINUS_SIGN:
            newTermOp.op = ValueExpr::MINUS;
            break;
        default: 
            newTermOp.op = ValueExpr::UNKNOWN;
            throw std::string("unhandled term_op type.");
        }
        a = op->getNextSibling();
        expr->_termOps.push_back(newTermOp);
    }
    return expr;

#endif
}
}}} // lsst::qserv::master

