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
#include "lsst/qserv/master/ColumnRefH.h"
#include "lsst/qserv/master/ColumnRef.h"
#include "lsst/qserv/master/FuncExpr.h"
#include "lsst/qserv/master/parseTreeUtil.h"
#include "lsst/qserv/master/ValueExpr.h" // For ValueExpr, FuncExpr
#include "SqlSQL2TokenTypes.hpp" 


// namespace modifiers
namespace qMaster = lsst::qserv::master;
using qMaster::ValueExpr;
using qMaster::FuncExpr;
using qMaster::ValueExprFactory;
using qMaster::ColumnRef;
using qMaster::ColumnRefMap;
using qMaster::tokenText;
using antlr::RefAST;

////////////////////////////////////////////////////////////////////////
// Anonymous helpers
////////////////////////////////////////////////////////////////////////
namespace {

inline RefAST walkToSiblingBefore(RefAST node, int typeId) {
    RefAST last = node;
    for(; node.get(); node = node->getNextSibling()) {
        if(node->getType() == typeId) return last;
        last = node;
    }
    return RefAST();
}

inline std::string getSiblingStringBounded(RefAST left, RefAST right) {
    qMaster::CompactPrintVisitor<RefAST> p;
    for(; left.get(); left = left->getNextSibling()) {
        p(left);
        if(left == right) break;
    }
    return p.result;
}

boost::shared_ptr<ValueExpr> 
newColumnExpr(antlr::RefAST expr, ColumnRefMap& cMap) {
    RefAST child = expr->getFirstChild();
    boost::shared_ptr<ValueExpr> ve(new ValueExpr());
    boost::shared_ptr<FuncExpr> fe;
    RefAST last;
    switch(expr->getType()) {
    case SqlSQL2TokenTypes::REGULAR_ID: 
        // make column ref. (no further children)
        {
            ColumnRefMap::Map::const_iterator it = cMap.map.find(expr);
            assert(it != cMap.map.end()); // Consider an exception instead
            ColumnRefMap::Ref r = it->second;
            boost::shared_ptr<ColumnRef> newColumnRef;
            newColumnRef.reset(new qMaster::ColumnRef(tokenText(r.db),
                                                      tokenText(r.table),
                                                      tokenText(r.column)));
            ve = ValueExpr::newColumnRefExpr(newColumnRef);
        }
        return ve;    
    case SqlSQL2TokenTypes::FUNCTION_SPEC:
        //        std::cout << "col child (fct): " << child->getType() << " "
        //                  << child->getText() << std::endl;
        fe.reset(new FuncExpr());
        last = walkToSiblingBefore(child, SqlSQL2TokenTypes::LEFT_PAREN);
        fe->name = getSiblingStringBounded(child, last);
        last = last->getNextSibling(); // Advance to LEFT_PAREN
        assert(last.get());
        // Now fill params. 
        for(antlr::RefAST current = last->getNextSibling();
            current.get(); current = current->getNextSibling()) {
            // Should be a * or a value expr.
            boost::shared_ptr<ValueExpr> pve;
            switch(current->getType()) {
            case SqlSQL2TokenTypes::VALUE_EXP: 
                pve = newColumnExpr(current->getFirstChild(), cMap);
                break;
            case SqlSQL2TokenTypes::COMMA: continue;
            case SqlSQL2TokenTypes::RIGHT_PAREN: continue;
            default: break;
            }
            fe->params.push_back(pve);
        }
        ve = ValueExpr::newFuncExpr(fe);
        return ve;
        
        break;
    default: 
        // throw ParseException(expr);// FIXME, need to unify exceptions.
        break;
    }
    return boost::shared_ptr<ValueExpr>();
}

boost::shared_ptr<ValueExpr> 
newSetFctSpec(RefAST expr, ColumnRefMap& cMap) {
    boost::shared_ptr<FuncExpr> fe(new FuncExpr());
    //    std::cout << "set_fct_spec " << walkTreeString(expr) << std::endl;
    RefAST nNode = expr->getFirstChild();
    assert(nNode.get());
    fe->name = nNode->getText();
    // Now fill params.
    antlr::RefAST current = nNode->getFirstChild();
    // Aggregation functions can only have one param.
    assert(current->getType() == SqlSQL2TokenTypes::LEFT_PAREN); 
    current = current->getNextSibling();
    // Should be a * or a value expr.
    boost::shared_ptr<ValueExpr> pve;
    switch(current->getType()) {
    case SqlSQL2TokenTypes::VALUE_EXP: 
        pve = newColumnExpr(current->getFirstChild(), cMap);
        break;
    case SqlSQL2TokenTypes::ASTERISK: 
        pve = ValueExpr::newStarExpr("");
        break;
    default: break;
    }
    current = current->getNextSibling();
    assert(current->getType() == SqlSQL2TokenTypes::RIGHT_PAREN); 
    fe->params.push_back(pve);
    return ValueExpr::newAggExpr(fe);
}

boost::shared_ptr<ValueExpr> 
newConstExpr(RefAST expr) {
    return ValueExpr::newConstExpr(qMaster::walkTreeString(expr));
}


} // anonymous
////////////////////////////////////////////////////////////////////////
// ValueExprFactory implementation
////////////////////////////////////////////////////////////////////////
ValueExprFactory::ValueExprFactory(boost::shared_ptr<ColumnRefMap> cMap) 
    : _columnRefMap(cMap) {
}

boost::shared_ptr<ValueExpr> 
ValueExprFactory::newExpr(antlr::RefAST a) {
    assert(_columnRefMap.get());
    boost::shared_ptr<ValueExpr> ve;
    switch(a->getType()) {
    case SqlSQL2TokenTypes::REGULAR_ID:
    case SqlSQL2TokenTypes::FUNCTION_SPEC:
        ve = newColumnExpr(a, *_columnRefMap);
        break;
    case SqlSQL2TokenTypes::SET_FCT_SPEC:
        ve = newSetFctSpec(a, *_columnRefMap);
        break;
    default: 
        ve = newConstExpr(a);
        break;
    }
    assert(ve.get());
    return ve;
}


