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

// C++
#include <deque>
#include <iterator>

// Package
#include "SqlSQL2Parser.hpp" // applies several "using antlr::***".
#include "lsst/qserv/master/ColumnRefH.h"

#include "lsst/qserv/master/SelectStmt.h"

#include "lsst/qserv/master/SelectListFactory.h" 
#include "lsst/qserv/master/SelectList.h" 
#include "lsst/qserv/master/ParseAliasMap.h" 

#include "lsst/qserv/master/parseTreeUtil.h"

#include "lsst/qserv/master/TableRefN.h"
// namespace modifiers
namespace qMaster = lsst::qserv::master;


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

class ParamGenerator {
public:
    struct Check {
        bool operator()(RefAST r) {
            return (r->getType() == SqlSQL2TokenTypes::RIGHT_PAREN) 
                || (r->getType() == SqlSQL2TokenTypes::COMMA);
        }
    };
    class Iter  {
    public:
        typedef std::forward_iterator_tag iterator_category;
        typedef std::string value_type;
        typedef int difference_type;
        typedef std::string* pointer;
        typedef std::string& reference;

        RefAST start;
        RefAST current;
        RefAST nextCache;
        Iter operator++(int) {
            //std::cout << "advancingX..: " << current->getText() << std::endl;
            Iter tmp = *this;
            ++*this;
            return tmp;
        }
        Iter& operator++() {
            //std::cout << "advancing..: " << current->getText() << std::endl;
            Check c;
            if(nextCache.get()) { 
                current = nextCache; 
            } else {
                current = qMaster::findSibling(current, c);
                if(current.get()) {
                    // Move to next value
                    current = current->getNextSibling(); 
                }
            }
            return *this; 
        }

        std::string operator*() {
            Check c;
            assert(current.get());
            qMaster::CompactPrintVisitor<antlr::RefAST> p;
            for(;current.get() && !c(current); 
                current = current->getNextSibling()) {
                p(current);                
            }
            return p.result;
        }
        bool operator==(Iter const& rhs) const {
            return (start == rhs.start) && (current == rhs.current);
        }
        bool operator!=(Iter const& rhs) const {
            return !(*this == rhs);
        }

    };
    ParamGenerator(RefAST a) {
        _beginIter.start = a;
        if(a.get() && (a->getType() == SqlSQL2TokenTypes::LEFT_PAREN)) {
            _beginIter.current = a->getNextSibling(); // Move past paren.
        } else { // else, set current as end.
            _beginIter.current = RefAST();
        }
        _endIter.start = a;
        _endIter.current = RefAST();
    }

    Iter begin() {
        return _beginIter;
    }

    Iter end() {
        return _endIter;
    }
private:
    Iter _beginIter;
    Iter _endIter;
};
} // anonymous
////////////////////////////////////////////////////////////////////////
// TableRefN misc impl. (to be placed in TableRefN.cc later)
////////////////////////////////////////////////////////////////////////
std::ostream& qMaster::operator<<(std::ostream& os, qMaster::TableRefN const& refN) {
    return refN.putStream(os);
}
////////////////////////////////////////////////////////////////////////
// ParseAliasMap misc impl. (to be placed in ParseAliasMap.cc later)
////////////////////////////////////////////////////////////////////////
std::ostream& qMaster::operator<<(std::ostream& os, 
                                  qMaster::ParseAliasMap const& m) {
    using qMaster::ParseAliasMap;
    typedef ParseAliasMap::Miter Miter;
    os << "AliasMap fwd(";
    for(Miter it=m._map.begin(); it != m._map.end(); ++it) {
        os << it->first->getText() << "->" << it->second->getText()
           << ", ";
    }
    os << ")    rev(";
    for(Miter it=m._rMap.begin(); it != m._rMap.end(); ++it) {
        os << it->first->getText() << "->" << it->second->getText()
           << ", ";
    }
    os << ")";
    return os;
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
        _f._import(a); // Trigger from list construction
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
class QualifiedName {
public:    
    QualifiedName(antlr::RefAST qn) {
        for(; qn.get(); qn = qn->getNextSibling()) {
            if(qn->getType() == SqlSQL2TokenTypes::PERIOD) continue;
            names.push_back(qn->getText());            
        }
    }
    std::string getQual(int i) const {
        return names[names.size() -1 - i];
    }
    std::string getName() const { return getQual(0); }
    std::deque<std::string> names;
};
////////////////////////////////////////////////////////////////////////
// FromFactory::ListIterator
////////////////////////////////////////////////////////////////////////
class FromFactory::RefGenerator {
public: 
    RefGenerator(antlr::RefAST firstRef,
                 boost::shared_ptr<ParseAliasMap> aliases) 
        : _cursor(firstRef), _aliases(aliases) {
        std::cout << *_aliases << std::endl;
        
    }
    TableRefN::Ptr get() const {
        assert(_cursor->getType() == SqlSQL2TokenTypes::TABLE_REF);
        RefAST node = _cursor->getFirstChild();
        RefAST child;

        TableRefN::Ptr tn;
        
        switch(node->getType()) {
        case SqlSQL2TokenTypes::TABLE_REF_AUX:
            child = node->getFirstChild();
            switch(child->getType()) {
            case SqlSQL2TokenTypes::QUALIFIED_NAME:
                tn.reset(_processQualifiedName(child));
                break;
            default:
                break;
            }
            break;
            // FIXME
        default:
            break;
        }
        return tn;
    }
    void next() {
        _cursor = _cursor->getNextSibling();
        if(!_cursor.get()) return; // Iteration complete
        switch(_cursor->getType()) {
        case SqlSQL2TokenTypes::COMMA:
            next();
            break;
        default:
            break;
        }
    }
    bool isDone() const {
        return !_cursor.get();
    }
private:
    void _setup() {
        // Sanity check: make sure we were constructed with a TABLE_REF
        if(_cursor->getType() == SqlSQL2TokenTypes::TABLE_REF) {
            // Nothing else to do
        } else {
            _cursor = RefAST(); // Clear out cursor so that isDone == true
        }
    }
    SimpleTableN* _processQualifiedName(RefAST n) const {
        RefAST qnStub = n;
        RefAST aliasN = _aliases->getAlias(qnStub);
        std::string alias;
        if(aliasN.get()) alias = aliasN->getText();
        QualifiedName qn(n->getFirstChild());
        if(qn.names.size() > 1) {
            return new SimpleTableN(qn.getQual(1), qn.getName(), alias);
        } else {
            return new SimpleTableN("", qn.getName(), alias);
        }
    }

    // Fields
    antlr::RefAST _cursor;
    boost::shared_ptr<ParseAliasMap> _aliases;
                 
};
////////////////////////////////////////////////////////////////////////
// FromFactory (impl)
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
    
    for(RefGenerator refGen(a, _aliases); !refGen.isDone(); refGen.next()) {
        TableRefN::Ptr p = refGen.get();
        TableRefN& tn = *p;
    }
    
    // FIXME: walk the tree and add elements.
}

////////////////////////////////////////////////////////////////////////
// WhereFactory::WhereCondH
////////////////////////////////////////////////////////////////////////
class lsst::qserv::master::WhereFactory::WhereCondH : public VoidOneRefFunc {
public:
    WhereCondH(lsst::qserv::master::WhereFactory& wf) : _wf(wf) {}
    virtual ~WhereCondH() {}
    virtual void operator()(antlr::RefAST where) {
        _wf._import(where);
    }
private:
    lsst::qserv::master::WhereFactory& _wf;
};
////////////////////////////////////////////////////////////////////////
// WhereFactory
////////////////////////////////////////////////////////////////////////
using qMaster::WhereClause;
using qMaster::WhereFactory;
WhereFactory::WhereFactory() {
    // FIXME
}

boost::shared_ptr<WhereClause> WhereFactory::getProduct() {
    // FIXME
    return boost::shared_ptr<WhereClause>(new WhereClause());
}

void 
WhereFactory::attachTo(SqlSQL2Parser& p) {
    boost::shared_ptr<WhereCondH> wch(new WhereCondH(*this));
    p._whereCondHandler = wch;
#if 0
    // Should trigger when the WHERE clause is detected.
    // Modifiers too? (ORDER, GROUP, LIMIT)
#endif
}

void 
WhereFactory::_import(antlr::RefAST a) {
    std::cout << "WHERE starts with: " << a->getText() 
              << " (" << a->getType() << ")" << std::endl;    

    std::cout << "WHERE indented: " << walkIndentedString(a) << std::endl;
    assert(a->getType() == SqlSQL2TokenTypes::SQL2RW_where);
    RefAST first = a->getFirstChild();
    assert(first.get());
    switch(first->getType()) {
    case SqlSQL2TokenTypes::QSERV_FCT_SPEC:
        while(first->getType() == SqlSQL2TokenTypes::QSERV_FCT_SPEC) {
            _addQservRestrictor(first->getFirstChild());
            first = first->getNextSibling();
            if(first.get()) {
                std::cout << "connector: " << first->getText() << "("
                          << first->getType() << ")" << std::endl;
                first = first->getNextSibling();
                assert(first.get());
            } else { break; }
        }
        _addOrSibs(first->getFirstChild());
        break;
    case SqlSQL2TokenTypes::OR_OP:
        _addOrSibs(first->getFirstChild());
        break;
    default:
        break;
    }
    // FIXME: walk the tree and add elements.
}
void 
WhereFactory::_addQservRestrictor(antlr::RefAST a) {
    std::string r(a->getText()); // e.g. qserv_areaspec_box
    std::cout << "Adding from " << r << " : ";
    ParamGenerator pg(a->getNextSibling());
    std::vector<std::string> params;

    // for(ParamGenerator::Iter it = pg.begin();
    //     it != pg.end();
    //     ++it) {
    //     std::cout << "iterating:" << *it << std::endl;
    // }
    std::copy(pg.begin(), pg.end(), std::back_inserter(params));
    std::copy(params.begin(),params.end(),
              std::ostream_iterator<std::string>(std::cout,", "));
    // FIXME: add restrictor spec.
}
template <typename Check>
struct PrintExcept : public qMaster::PrintVisitor<antlr::RefAST> {
public:
    PrintExcept(Check c_) : c(c_) {}
    void operator()(antlr::RefAST a) {
        if(!c(a)) qMaster::PrintVisitor<antlr::RefAST>::operator()(a);
    }
    Check& c;
};
struct MetaCheck {
    bool operator()(antlr::RefAST a) {
        if(!a.get()) return false;
        switch(a->getType()) {
        case SqlSQL2TokenTypes::OR_OP:
        case SqlSQL2TokenTypes::AND_OP:
        case SqlSQL2TokenTypes::BOOLEAN_FACTOR:
        case SqlSQL2TokenTypes::VALUE_EXP:
            return true;
        default:
            return false;
        }
        return false;
    }
};

void 
WhereFactory::_addOrSibs(antlr::RefAST a) {
    MetaCheck mc;
    PrintExcept<MetaCheck> p(mc);
    walkTreeVisit(a, p);
    std::cout << "Adding orsibs: " << p.result << std::endl;
    //std::cout << "(old): " << walkTreeString(a) << std::endl;
    //std::cout << "(indent): " << walkIndentedString(a) << std::endl;
}
