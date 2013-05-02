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
// ValueTermFactory.cc constructs ValueTerm instances from an ANTLR
// subtrees.

#include "lsst/qserv/master/ValueTermFactory.h"
#include "lsst/qserv/master/ColumnRefH.h"
#include "lsst/qserv/master/ColumnRef.h"
#include "lsst/qserv/master/FuncExpr.h"
#include "lsst/qserv/master/parseTreeUtil.h"
#include "lsst/qserv/master/ValueExpr.h" // For ValueExpr
#include "lsst/qserv/master/ValueTerm.h" // For ValueTerm
#include "SqlSQL2TokenTypes.hpp" 


// namespace modifiers
namespace qMaster = lsst::qserv::master;
using qMaster::ValueTerm;
using qMaster::ValueExpr;
using qMaster::FuncExpr;
using qMaster::ValueTermFactory;
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

boost::shared_ptr<ValueTerm> 
newColumnTerm(antlr::RefAST t, ColumnRefMap& cMap) {
    RefAST child = t->getFirstChild();
    boost::shared_ptr<ValueTerm> vt(new ValueTerm());
    boost::shared_ptr<FuncExpr> fe;
    RefAST last;
    switch(t->getType()) {
    case SqlSQL2TokenTypes::REGULAR_ID: 
        // make column ref. (no further children)
        {
            ColumnRefMap::Map::const_iterator it = cMap.map.find(t);
            assert(it != cMap.map.end()); // Consider an exception instead
            ColumnRefMap::Ref r = it->second;
            boost::shared_ptr<ColumnRef> newColumnRef;
            newColumnRef.reset(new qMaster::ColumnRef(tokenText(r.db),
                                                      tokenText(r.table),
                                                      tokenText(r.column)));
            vt = ValueTerm::newColumnRefTerm(newColumnRef);
        }
        return vt;    
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
            boost::shared_ptr<ValueTerm> pvt;
            switch(current->getType()) {
            case SqlSQL2TokenTypes::VALUE_EXP: 
                pvt = newColumnTerm(current->getFirstChild(), cMap);
                break;
            case SqlSQL2TokenTypes::COMMA: continue;
            case SqlSQL2TokenTypes::RIGHT_PAREN: continue;
            default: break;
            }
            fe->params.push_back(ValueExpr::newSimple(pvt));
        }
        vt = ValueTerm::newFuncTerm(fe);
        return vt;
        
        break;
    default: 
        // throw ParseException(expr);// FIXME, need to unify exceptions.
        break;
    }
    return boost::shared_ptr<ValueTerm>();
}

boost::shared_ptr<ValueTerm> 
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
    boost::shared_ptr<ValueTerm> pvt;
    switch(current->getType()) {
    case SqlSQL2TokenTypes::VALUE_EXP: 
        pvt = newColumnTerm(current->getFirstChild(), cMap);
        break;
    case SqlSQL2TokenTypes::ASTERISK: 
        pvt = ValueTerm::newStarTerm("");
        break;
    default: break;
    }
    current = current->getNextSibling();
    assert(current->getType() == SqlSQL2TokenTypes::RIGHT_PAREN); 
    fe->params.push_back(ValueExpr::newSimple(pvt));
    return ValueTerm::newAggTerm(fe);
}

boost::shared_ptr<ValueTerm> 
newConstTerm(RefAST t) {
    return ValueTerm::newConstTerm(qMaster::walkTreeString(t));
}


} // anonymous
namespace lsst { namespace qserv { namespace master { 
////////////////////////////////////////////////////////////////////////
// ValueTermFactory implementation
////////////////////////////////////////////////////////////////////////
ValueTermFactory::ValueTermFactory(boost::shared_ptr<ColumnRefMap> cMap) 
    : _columnRefMap(cMap) {
}


/* VALUE_EXP               */
/* |             \         */
/* TERM   (TERM_OP TERM)*  */
boost::shared_ptr<ValueTerm> 
ValueTermFactory::newTerm(antlr::RefAST a) {
    assert(_columnRefMap.get());
    boost::shared_ptr<ValueTerm> vt;
    int eType = a->getType();
    if(a->getType() == SqlSQL2TokenTypes::TERM) {
        a = a->getFirstChild(); // TERM is a parent placeholder element
    }
    std::cout << "new term: " << tokenText(a) << std::endl;
    switch(a->getType()) {
    case SqlSQL2TokenTypes::REGULAR_ID:
    case SqlSQL2TokenTypes::FUNCTION_SPEC:
        vt = newColumnTerm(a, *_columnRefMap);
        break;
    case SqlSQL2TokenTypes::SET_FCT_SPEC:
        vt = newSetFctSpec(a, *_columnRefMap);
        break;
    default: 
        vt = newConstTerm(a);
        break;
    }
    assert(vt.get());
    return vt;
}
}}} // lsst::qserv::master
