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

// SelectParser is the top-level manager for everything attached to
// parsing the top-level SQL query. Given an input query and a
// configuration, computes a query info structure, name ref list, and
// a "query plan".  

#if 0
// Standard
#include <functional>
#include <cstdio>
#include <strings.h>

// Boost
#include <boost/make_shared.hpp>
#include <boost/bind.hpp>
#endif //comment out

#include <map>
//#include <antlr/AST.hpp>

// Local (placed in src/)
#include "SqlSQL2Parser.hpp" 

#include "lsst/qserv/master/parseTreeUtil.h"
// myself
#include "lsst/qserv/master/SelectStmt.h"

// namespace modifiers
namespace qMaster = lsst::qserv::master;


////////////////////////////////////////////////////////////////////////
// Experimental
////////////////////////////////////////////////////////////////////////
typedef std::pair<antlr::RefAST, antlr::RefAST> NodeBound;
typedef std::map<antlr::RefAST, NodeBound> NodeMap;

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
            NodeBound target(a, getSiblingBefore(a,b));
            std::cout << "column map " << walkTreeString(b) 
                      << " --> "
                      <<  walkTree(a)
                      << std::endl;
            //_am._columnAliasNodeMap[a] = NodeBound(b, getLastSibling(a));
        }
        std::cout << "column node " 
                  << walkBoundedTreeString(a, getLastSibling(a))
                  << std::endl;
        
        //_am._columnAliasNodes.push_back(NodeBound(a, getLastSibling(a)));
        // Save column ref for pass/fixup computation, 
        // regardless of alias.
    }
private:
}; // class ColumnAliasH

class ColumnRefH : public VoidFourRefFunc {
public:    
    ColumnRefH() {}
    virtual ~ColumnRefH() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b, 
                            antlr::RefAST c, antlr::RefAST d) {
        if(d.get()) {
            _process(c, d);
        } else if(c.get()) {
            _process(b, c);
        } else if(b.get()) {
            _process(a, b);
        } else { 
            _process(antlr::RefAST(), a); 
        }
        // std::cout << "col _" << tokenText(a) 
        //        << "_ _" << tokenText(b) 
        //        << "_ _" << tokenText(c) 
            //        << "_ _" << tokenText(d) 
            //        << "_ "; 
            // a->setText("AWESOMECOLUMN");
    }
private:
    inline void _process(antlr::RefAST t, antlr::RefAST c) {
        using qMaster::tokenText;
        std::cout << "columnref: table:" << tokenText(t)
                  << " column:" << tokenText(c) << std::endl;
    }
    
};

class SelectStarH : public VoidVoidFunc {
public: 
    virtual ~SelectStarH() {}
    virtual void operator()() {
        using lsst::qserv::master::getLastSibling;
        using qMaster::tokenText;
        using qMaster::walkBoundedTreeString;
        std::cout << "Found Select *" << std::endl;
    }
private:
}; // SelectStarH

class SelectListH : public VoidOneRefFunc {
public: 
    virtual ~SelectListH() {}
    virtual void operator()(RefAST a) {
        using qMaster::walkTreeString;
        std::cout << "Found Select List: " << walkTreeString(a) << std::endl;
    }
private:
}; // class ColumnAliasH

struct FromEntry {
    RefAST alias;
    NodeBound target;
    
};

class FromList {
public:
    int i;
};

////////////////////////////////////////////////////////////////////////
// class SelectStmt
////////////////////////////////////////////////////////////////////////


// Hook into parser to get populated.
void qMaster::SelectStmt::addHooks(SqlSQL2Parser& p) {
    p._columnAliasHandler.reset(new ColumnAliasH());
    p._columnRefHandler.reset(new ColumnRefH());
    p._selectStarHandler.reset(new SelectStarH());
    p._selectListHandler.reset(new SelectListH());
}
