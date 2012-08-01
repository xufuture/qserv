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
// SelectFactory houses the implementation of the SelectFactory, which
// is responsible (through some delegated behavior) for constructing
// SelectStmt (and SelectList, etc.) from an ANTLR parse tree.  For
// now, the code for other factories is included here as well.

#include "lsst/qserv/master/SelectFactory.h"
#include "SqlSQL2Parser.hpp" // applies several "using antlr::***".
#include "lsst/qserv/master/ColumnRefH.h"

#include "lsst/qserv/master/SelectStmt.h"

#include "lsst/qserv/master/SelectListFactory.h" 
#include "lsst/qserv/master/SelectList.h" 
#include "lsst/qserv/master/ParseAliasMap.h" 

#include "lsst/qserv/master/parseTreeUtil.h"
// namespace modifiers
namespace qMaster = lsst::qserv::master;


////////////////////////////////////////////////////////////////////////
// Anonymous helpers
////////////////////////////////////////////////////////////////////////
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
////////////////////////////////////////////////////////////////////////
// Parse handlers
////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////
// SelectFactory
////////////////////////////////////////////////////////////////////////
using qMaster::SelectFactory;
using qMaster::SelectListFactory;
using qMaster::SelectStmt;

SelectFactory::SelectFactory() 
    : _columnAliases(new ParseAliasMap()),
      _tableAliases(new ParseAliasMap()),
      _columnRefMap(new ColumnRefMap()),
      _slFactory(new SelectListFactory(_columnAliases, _columnRefMap)),
      _fFactory(new FromFactory(_tableAliases)),
      _wFactory(new WhereFactory()) {

}

void
SelectFactory::attachTo(SqlSQL2Parser& p) {
    _attachShared(p);
    
    _slFactory->attachTo(p);
    _fFactory->attachTo(p);
    _wFactory->attachTo(p);
}

boost::shared_ptr<SelectStmt>
SelectFactory::getStatement() {
    boost::shared_ptr<SelectStmt> stmt(new SelectStmt());
    stmt->_selectList = _slFactory->getProduct();
    stmt->_fromList = _fFactory->getProduct();
    stmt->_whereClause = _wFactory->getProduct();
    return stmt;
}

void 
SelectFactory::_attachShared(SqlSQL2Parser& p) {
    boost::shared_ptr<ColumnRefH> crh(new ColumnRefH());
    crh->setListener(_columnRefMap);
    p._columnRefHandler = crh;
}

////////////////////////////////////////////////////////////////////////
// SelectListFactory::SelectListH
////////////////////////////////////////////////////////////////////////
class SelectListFactory::SelectListH : public VoidOneRefFunc {
public: 
    explicit SelectListH(SelectListFactory& f) : _f(f) {}
    virtual ~SelectListH() {}
    virtual void operator()(RefAST a) {
        _f._import(a); // Trigger select list construction
    }
    SelectListFactory& _f;
};

////////////////////////////////////////////////////////////////////////
// SelectListFactory::SelectStarH
////////////////////////////////////////////////////////////////////////
class SelectListFactory::SelectStarH : public VoidOneRefFunc {
public: 
    explicit SelectStarH(SelectListFactory& f) : _f(f) {}
    virtual ~SelectStarH() {}
    virtual void operator()(antlr::RefAST a) {
        _f._addSelectStar();
    }
private:
    SelectListFactory& _f;
}; // SelectStarH


////////////////////////////////////////////////////////////////////////
// SelectListFactory::ColumnAliasH
////////////////////////////////////////////////////////////////////////
class SelectListFactory::ColumnAliasH : public VoidTwoRefFunc {
public: 
    ColumnAliasH(boost::shared_ptr<qMaster::ParseAliasMap> map) : _map(map) {}
    virtual ~ColumnAliasH() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b)  {
        using lsst::qserv::master::getSiblingBefore;
        using qMaster::tokenText;
        if(b.get()) {
            // qMaster::NodeBound target(a, getSiblingBefore(a,b));
            // // Exclude the "AS" 
            // if(boost::iequals(tokenText(target.second) , "as")) {
            //     target.second = getSiblingBefore(a, target.second);
            // }
            b->setType(SqlSQL2TokenTypes::COLUMN_ALIAS_NAME);
            _map->addAlias(b, a);
        }
        // Save column ref for pass/fixup computation, 
        // regardless of alias.
    }
private:
    boost::shared_ptr<qMaster::ParseAliasMap> _map;
}; // class ColumnAliasH


////////////////////////////////////////////////////////////////////////
// class SelectListFactory 
////////////////////////////////////////////////////////////////////////
using qMaster::SelectList;

SelectListFactory::SelectListFactory(boost::shared_ptr<ParseAliasMap> aliasMap,
                                     boost::shared_ptr<ColumnRefMap> crMap)
    : _aliases(aliasMap),
      _columnRefMap(crMap),
      _valueExprList(new ValueExprList()) {
}

void
SelectListFactory::attachTo(SqlSQL2Parser& p) {
    _selectListH.reset(new SelectListH(*this));
    _columnAliasH.reset(new ColumnAliasH(_aliases));
    p._selectListHandler = _selectListH;
    p._selectStarHandler.reset(new SelectStarH(*this));
    p._columnAliasHandler = _columnAliasH;
}

boost::shared_ptr<SelectList> SelectListFactory::getProduct() {
    boost::shared_ptr<SelectList> slist(new SelectList());
    slist->_valueExprList = _valueExprList;
    return slist;
}

void
SelectListFactory::_import(RefAST selectRoot) {
    //    std::cout << "Type of selectRoot is "
    //              << selectRoot->getType() << std::endl;

    for(; selectRoot.get();
        selectRoot = selectRoot->getNextSibling()) {
        RefAST child = selectRoot->getFirstChild();
        switch(selectRoot->getType()) {
        case SqlSQL2TokenTypes::SELECT_COLUMN:
            assert(child.get());
            _addSelectColumn(child);
            break;
        case SqlSQL2TokenTypes::SELECT_TABLESTAR:
            assert(child.get());
            _addSelectStar(child);
            break;
        case SqlSQL2TokenTypes::ASTERISK: // Not sure this will be
                                          // handled here.
            _addSelectStar();
            // should only have a single unqualified "*"            
            break;
        default:
            throw ParseException(selectRoot);
     
        } // end switch
    } // end for-each select_list child.
}

void
SelectListFactory::_addSelectColumn(RefAST expr) {
    ValueExprPtr e(new ValueExpr());
    // Figure out what type of value expr, and create it properly.
    // std::cout << "SelectCol Type of:" << expr->getText() 
    //           << "(" << expr->getType() << ")" << std::endl;
    if(expr->getType() != SqlSQL2TokenTypes::VALUE_EXP) {
        throw ParseException(expr);
    }
    RefAST child = expr->getFirstChild();
    assert(child.get());
    //    std::cout << "child is " << child->getType() << std::endl;
    ValueExprPtr ve;
    switch(child->getType()) {
    case SqlSQL2TokenTypes::REGULAR_ID:
    case SqlSQL2TokenTypes::FUNCTION_SPEC:
        ve = _newColumnExpr(child);
        break;
    case SqlSQL2TokenTypes::SET_FCT_SPEC:
        ve = _newSetFctSpec(child);
        break;
    default: break;
    }
    // Annotate if alias found.
    RefAST alias = _aliases->getAlias(expr);
    if(alias.get()) {
        ve->_alias = tokenText(alias);
    }
    _valueExprList->push_back(ve);
}

void
SelectListFactory::_addSelectStar(RefAST child) {
    // Note a "Select *".
    // If child.get(), this means that it's in the form of
    // "table.*". There might be sibling handling (i.e., multiple
    // table.* expressions).
    ValueExprPtr ve;
    if(child.get()) {
        // child should be QUALIFIED_NAME, so its child should be a
        // table name.
        RefAST table = child->getFirstChild();
        assert(table.get());
        std::cout << "table ref'd for *: " << tokenText(table) << std::endl;
        ve = ValueExpr::newStarExpr(qMaster::tokenText(table));
    } else {
        // just add * to the selectList.
        ve = ValueExpr::newStarExpr(std::string());
    }
    _valueExprList->push_back(ve);
}
qMaster::ValueExprPtr 
SelectListFactory::_newColumnExpr(RefAST expr) {
    RefAST child = expr->getFirstChild();
    //    std::cout << "new col expr has value " << walkSiblingString(expr)
    //              << std::endl;
    ValueExprPtr ve(new ValueExpr());
    boost::shared_ptr<FuncExpr> fe;
    RefAST last;
    switch(expr->getType()) {
    case SqlSQL2TokenTypes::REGULAR_ID: 
        // make column ref. (no further children)
        ve->_type = ValueExpr::COLUMNREF;
        {
            ColumnRefMap::Map::const_iterator it = _columnRefMap->map.find(expr);
            assert(it != _columnRefMap->map.end()); // Consider an exception instead
            ColumnRefMap::Ref r = it->second;
            ve->_columnRef.reset(new qMaster::ColumnRef(tokenText(r.db),
                                                    tokenText(r.table),
                                                    tokenText(r.column)));
        }
        return ve;    
    case SqlSQL2TokenTypes::FUNCTION_SPEC:
        //        std::cout << "col child (fct): " << child->getType() << " "
        //                  << child->getText() << std::endl;
        ve->_type = ValueExpr::FUNCTION;
        fe.reset(new FuncExpr());
        last = walkToSiblingBefore(child, SqlSQL2TokenTypes::LEFT_PAREN);
        fe->name = getSiblingStringBounded(child, last);
        last = last->getNextSibling(); // Advance to LEFT_PAREN
        assert(last.get());
        // Now fill params. 
        for(antlr::RefAST current = last->getNextSibling();
            current.get(); current = current->getNextSibling()) {
            // Should be a * or a value expr.
            ValueExprPtr pve;
            switch(current->getType()) {
            case SqlSQL2TokenTypes::VALUE_EXP: 
                pve = _newColumnExpr(current->getFirstChild());
                break;
            case SqlSQL2TokenTypes::COMMA: continue;
            case SqlSQL2TokenTypes::RIGHT_PAREN: continue;
            default: break;
            }
            fe->params.push_back(pve);            
        }
        ve->_funcExpr = fe;
        return ve;
        
        break;
    default: 
        throw ParseException(expr);
        break;
    }
    return ValueExprPtr();
}

qMaster::ValueExprPtr 
SelectListFactory::_newSetFctSpec(RefAST expr) {
    ValueExprPtr ve(new ValueExpr());
    boost::shared_ptr<FuncExpr> fe(new FuncExpr());
    //    std::cout << "set_fct_spec " << walkTreeString(expr) << std::endl;
    RefAST nNode = expr->getFirstChild();
    assert(nNode.get());
    fe->name = nNode->getText();
    // Now fill params.
    antlr::RefAST current = nNode->getFirstChild();
#if 1 // Aggregation functions can only have one param.
    assert(current->getType() == SqlSQL2TokenTypes::LEFT_PAREN); 
    current = current->getNextSibling();
    // Should be a * or a value expr.
    ValueExprPtr pve;
    switch(current->getType()) {
    case SqlSQL2TokenTypes::VALUE_EXP: 
        pve = _newColumnExpr(current->getFirstChild());
        break;
    case SqlSQL2TokenTypes::ASTERISK: 
        pve.reset(new ValueExpr());
        pve->_type = ValueExpr::STAR;
        break;
    default: break;
    }
    current = current->getNextSibling();
    assert(current->getType() == SqlSQL2TokenTypes::RIGHT_PAREN); 
    fe->params.push_back(pve);
#else
    //std::cout << "params got " << tokenText(pnodes) << std::endl;
    // Make sure the parser gave us the right nodes.

    for(current = current->getNextSibling(); 
        current.get(); 
        current=current->getNextSibling()) {
        if(current->getType() == SqlSQL2TokenTypes::COMMA) { continue; }
        if(current->getType() == SqlSQL2TokenTypes::RIGHT_PAREN) { break; }
        ValueExprPtr ve = _newColumnExpr(current);
        if(!ve.get()) {
            throw ParseException(current);
        }
        fe->params.push_back(ve);
    }
#endif
    // Now fill-out ValueExpr
    ve->_type = ValueExpr::AGGFUNC;
    ve->_funcExpr = fe;
    return ve;
}

////////////////////////////////////////////////////////////////////////
// class SelectListFactory::ParseException 
////////////////////////////////////////////////////////////////////////
SelectListFactory::ParseException::ParseException(RefAST subtree) :
    runtime_error("SelectList parse error") {
    // empty.
}

////////////////////////////////////////////////////////////////////////
// FromFactory
////////////////////////////////////////////////////////////////////////
using qMaster::FromList;
using qMaster::FromFactory;
////////////////////////////////////////////////////////////////////////
// FromFactory::FromClauseH
////////////////////////////////////////////////////////////////////////
class FromFactory::TableRefListH : public VoidTwoRefFunc {
public:
    TableRefListH(FromFactory& f) : _f(f) {}
    virtual ~TableRefListH() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b) {
        _f._import(a); // Trigger select list construction
    }    
private:
    FromFactory& _f;
};
////////////////////////////////////////////////////////////////////////
// FromFactory::TableRefAuxH
////////////////////////////////////////////////////////////////////////
class FromFactory::TableRefAuxH : public VoidFourRefFunc {
public: 
    TableRefAuxH(boost::shared_ptr<qMaster::ParseAliasMap> map) 
        : _map(map) {}
    virtual ~TableRefAuxH() {}
    virtual void operator()(antlr::RefAST name, antlr::RefAST sub, 
                            antlr::RefAST as, antlr::RefAST alias)  {
        using lsst::qserv::master::getSiblingBefore;
        using qMaster::tokenText;
        if(alias.get()) {
            _map->addAlias(alias, name);
        }
        // Save column ref for pass/fixup computation, 
        // regardless of alias.
    }
private:
    boost::shared_ptr<qMaster::ParseAliasMap> _map;
};

////////////////////////////////////////////////////////////////////////
// FromFactory
////////////////////////////////////////////////////////////////////////
FromFactory::FromFactory(boost::shared_ptr<ParseAliasMap> aliases) :
        _aliases(aliases) {
}

boost::shared_ptr<FromList> 
FromFactory::getProduct() {
    // FIXME
    return boost::shared_ptr<FromList>(new FromList());
}

void 
FromFactory::attachTo(SqlSQL2Parser& p) {
    boost::shared_ptr<TableRefListH> lh(new TableRefListH(*this));
    p._tableListHandler = lh;
    boost::shared_ptr<TableRefAuxH> ah(new TableRefAuxH(_aliases));
    p._tableAliasHandler = ah;
}

void 
FromFactory::_import(antlr::RefAST a) {
    std::cout << "FROM starts with: " << a->getText() 
              << " (" << a->getType() << ")" << std::endl;
}

////////////////////////////////////////////////////////////////////////
// WhereFactory
////////////////////////////////////////////////////////////////////////
using qMaster::WhereClause;
using qMaster::WhereFactory;
boost::shared_ptr<WhereClause> WhereFactory::getProduct() {
    // FIXME
    return boost::shared_ptr<WhereClause>(new WhereClause());
}


