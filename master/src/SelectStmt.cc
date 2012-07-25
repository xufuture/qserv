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

// SelectStmt is the query info structure. It contains information
// about the top-level query characteristics. It shouldn't contain
// information about run-time query execution.  It might contain
// enough information to generate queries for execution.


#if 0
// Standard
#include <functional>
#include <cstdio>
#include <strings.h>

#include <boost/bind.hpp>
#endif //comment out


// Standard
#include <map>
//#include <antlr/AST.hpp>

// Boost
//#include <boost/make_shared.hpp>

#include <boost/algorithm/string/predicate.hpp> // string iequal


// Local (placed in src/)
#include "SqlSQL2Parser.hpp" 
#include "SqlSQL2Tokens.h"
#include "SqlSQL2TokenTypes.hpp"

#include "lsst/qserv/master/parseTreeUtil.h"
#include "lsst/qserv/master/ColumnRefH.h"
#include "lsst/qserv/master/SelectList.h"
// myself
#include "lsst/qserv/master/SelectStmt.h"

// namespace modifiers
namespace qMaster = lsst::qserv::master;


////////////////////////////////////////////////////////////////////////
// Experimental
////////////////////////////////////////////////////////////////////////

// forward

////////////////////////////////////////////////////////////////////////
// class SelectStmt::Mgr
////////////////////////////////////////////////////////////////////////
class qMaster::SelectStmt::Mgr {
public:
    enum Phrase {SELECTP, FROMP, WHEREP, POST};
    Mgr(SelectStmt& stmt) 
        : _stmt(stmt), _phrase(SELECTP) {
    }

    void addColumnAlias(RefAST label, NodeBound target) {
        _columnAliases[label] = target;
    }
    boost::shared_ptr<VoidFourRefFunc> getColumnRefH();
    boost::shared_ptr<VoidOneRefFunc> getSelectStarH();
    boost::shared_ptr<VoidOneRefFunc> getSelectListH();
    class SelectStarH;
    class SelectListH;
    friend class SelectStarH;
    friend class SelectListH;
    void setSelectFinish() {
        _phrase = FROMP;
        // change listener for column refs 
        _columnRefH->setListener(_stmt._fromList->getColumnRefList());
    }
    void setFromFinish() {
        _phrase = FROMP;
        // change listener for column refs 
        _columnRefH->setListener(_stmt._whereClause->getColumnRefList());
    }
    
private:
    void _setupColumnRefH();
    SelectStmt& _stmt;
    Phrase _phrase;
    NodeMap _columnAliases;
    boost::shared_ptr<ColumnRefH> _columnRefH;

};
class qMaster::SelectStmt::Mgr::SelectStarH : public VoidOneRefFunc {
public: 
    explicit SelectStarH(Mgr& m) : _mgr(m) {}
    virtual ~SelectStarH() {}
    virtual void operator()(antlr::RefAST a) {
        using lsst::qserv::master::getLastSibling;
        using qMaster::tokenText;
        using qMaster::walkBoundedTreeString;
        std::cout << "Found Select *" << std::endl;
        _mgr._stmt.addSelectStar();
        _mgr.setSelectFinish();
    }
private:
    Mgr& _mgr;
}; // SelectStarH

class qMaster::SelectStmt::Mgr::SelectListH : public VoidOneRefFunc {
public: 
    explicit SelectListH(qMaster::SelectStmt::Mgr& m) : _mgr(m) {}
    virtual ~SelectListH() {}
    virtual void operator()(RefAST a) {
        using qMaster::walkTreeString;
        std::cout << "Found Select List("
                  << a->getType() << "): " 
                  << walkSiblingString(a) << std::endl;
        _import(a);
        _mgr.setSelectFinish();
    }
    void _importValue(RefAST a) {
        switch(a->getType()) {
        case SqlSQL2TokenTypes::SQL2RW_count:
        case SqlSQL2TokenTypes::SQL2RW_avg:
        case SqlSQL2TokenTypes::SQL2RW_max:
        case SqlSQL2TokenTypes::SQL2RW_min:
        case SqlSQL2TokenTypes::SQL2RW_sum:
            std::cout << "Aggregation detected." << tokenText(a) << std::endl;
            _mgr._stmt._selectList->addAgg(a);
            break;
        case SqlSQL2TokenTypes::REGULAR_ID:
            std::cout << "RegularId." << tokenText(a) << std::endl;
            _mgr._stmt._selectList->addRegular(a);
            break;
        case SqlSQL2TokenTypes::FUNCTION_SPEC:
            std::cout << "fspec." << tokenText(a) << std::endl;
            _mgr._stmt._selectList->addFunc(a);
            break;
        default:
            std::cout << "FIXME handle ("<< a->getType() 
                      << ")" << tokenText(a) << std::endl;
            break;
        }
    }
    void _importTableStar(RefAST a) {
        // FIXME
    }
    void _import(RefAST a) {
        for(; a.get(); a=a->getNextSibling()) {
            if(a->getType() != ANTLR_SELECT_COLUMN) {
                if(a->getType() == ANTLR_SELECT_TABLESTAR) {
                    _importTableStar(a);
                } else {
                    std::cout << "Warning, unhandled parse element:" 
                              << walkTreeString(a) << std::endl;
                    continue;
                }
            }
            //_mgr._stmt->_selectList;
            RefAST node = a->getFirstChild();
            switch(node->getType()) {
            case ANTLR_VALUE_EXP:
                _importValue(node->getFirstChild());
                break;
            default:
                std::cout << "Warning, Unsupported SELECT_COLUMN child:" 
                          << walkTreeString(node) << std::endl;
                break;
            }
        }        
    }
private:
    Mgr& _mgr;
}; // SelectListH
boost::shared_ptr<VoidOneRefFunc> qMaster::SelectStmt::Mgr::getSelectStarH() {
    // non-const denies make_shared.
    return boost::shared_ptr<SelectStarH>(new SelectStarH(*this));
}
boost::shared_ptr<VoidOneRefFunc> qMaster::SelectStmt::Mgr::getSelectListH() {
    return boost::shared_ptr<SelectListH>(new SelectListH(*this));
}
void qMaster::SelectStmt::Mgr::_setupColumnRefH() {
    _columnRefH.reset(new ColumnRefH()); 
    _columnRefH->setListener(_stmt._selectList->getColumnRefList());
}
boost::shared_ptr<VoidFourRefFunc> qMaster::SelectStmt::Mgr::getColumnRefH() {
    if(!_columnRefH.get()) { _setupColumnRefH(); }
    return _columnRefH;
}

////////////////////////////////////////////////////////////////////////
// Handlers
////////////////////////////////////////////////////////////////////////
class ColumnAliasH : public VoidTwoRefFunc {
public: 
    virtual ~ColumnAliasH() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b)  {
        using lsst::qserv::master::getLastSibling;
        using lsst::qserv::master::getSiblingBefore;
        using qMaster::tokenText;
        using qMaster::walkBoundedTreeString;
        using qMaster::walkTreeString;
        using qMaster::walkTree;
        if(b.get()) {
            qMaster::NodeBound target(a, getSiblingBefore(a,b));
            // Exclude the "AS" 
            if(boost::iequals(tokenText(target.second) , "as")) {
                target.second = getSiblingBefore(a, target.second);
            }
            // std::cout << "column map " << walkTreeString(b) 
            //           << " --> "
            //           <<  walkBoundedTreeString(target.first, target.second)
            //           << std::endl;
            //_am._columnAliasNodeMap[a] = NodeBound(b, getLastSibling(a));
        }

#if 0
        std::cout << "column node " 
                  << walkBoundedTreeString(a, getLastSibling(a))
                  << std::endl;
#endif
        // Don't really need column node here-- column ref captures better.
        
        //_am._columnAliasNodes.push_back(NodeBound(a, getLastSibling(a)));
        // Save column ref for pass/fixup computation, 
        // regardless of alias.
    }
private:
}; // class ColumnAliasH

class SetFctSpecH : public VoidOneRefFunc {
public:
    virtual ~SetFctSpecH() {}
    virtual void operator()(antlr::RefAST a)  {
        // FIXME: Put parameters underneath function name node.
        
        // FIXME: Create function call node.
    }    
    
};

class FunctionSpecH : public VoidTwoRefFunc {
public:
    virtual ~FunctionSpecH() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b)  {
        using qMaster::walkTreeString;
        using qMaster::tokenText;
        // Make into a tree
        // Make b a child of a
        a->setFirstChild(b);
        // Look for b among a's siblings and excise b from the list.
        RefAST bSib = a;
        RefAST next = bSib->getNextSibling();
        while(next.get()) {
            if(next == b) {
                bSib->setNextSibling(RefAST()); 
                break;
            }
            next = next->getNextSibling();
        } 
        RefAST current = b;
        next = current->getNextSibling();;
        while(next.get()) {
            if(tokenText(next) == ")") {
                current->setNextSibling(RefAST());
                break;
            }
            current = next;
            next = next->getNextSibling();;
            
        }
        std::cout << "recv func:" << walkTreeString(a) << "--"
                  << walkTreeString(b) << std::endl;

        // FIXME: Create function call node.
        //addFuncExpr(a,b);
    }
    
};

////////////////////////////////////////////////////////////////////////
// class SelectStmt
////////////////////////////////////////////////////////////////////////

// Hook into parser to get populated.
qMaster::SelectStmt::SelectStmt() 
    : _mgr(new Mgr(*this)),
    _fromList(new FromList()),
    _selectList(new SelectList()),
    _whereClause(new WhereClause()) {
    // ---
}
// Hook into parser to get populated.
void qMaster::SelectStmt::addHooks(SqlSQL2Parser& p) {
    p._columnAliasHandler.reset(new ColumnAliasH());
    p._columnRefHandler = _mgr->getColumnRefH();
    p._selectStarHandler = _mgr->getSelectStarH();
    p._selectListHandler = _mgr->getSelectListH();
    //p._functionSpecHandler.reset(new FunctionSpecH());

}

void qMaster::SelectStmt::addSelectStar() {
//    _selectList->addStar();
}

void qMaster::SelectStmt::diagnose() {
    _selectList->getColumnRefList()->printRefs();
    //_selectList->dbgPrint();
}

