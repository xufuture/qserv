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
#include "SqlSQL2Parser.hpp"

#include "lsst/qserv/master/SelectListFactory.h" 
#include "lsst/qserv/master/ParseAliasMap.h" 

#include "lsst/qserv/master/parseTreeUtil.h"
// namespace modifiers
namespace qMaster = lsst::qserv::master;

////////////////////////////////////////////////////////////////////////
// Parse handlers
////////////////////////////////////////////////////////////////////////
class SelectStarH : public VoidOneRefFunc {
public: 
    explicit SelectStarH() {}
    virtual ~SelectStarH() {}
    virtual void operator()(antlr::RefAST a);
 // {
 //        using lsst::qserv::master::getLastSibling;
 //        using qMaster::tokenText;
 //        using qMaster::walkBoundedTreeString;
 //        std::cout << "Found Select *" << std::endl;
 //        _mgr._stmt.addSelectStar(RefAST());
 //        _mgr.setSelectFinish();
 //    }
private:
//    Mgr& _mgr;
}; // SelectStarH

////////////////////////////////////////////////////////////////////////
// SelectFactory
////////////////////////////////////////////////////////////////////////
using qMaster::SelectFactory;
using qMaster::SelectListFactory;

SelectFactory::SelectFactory() 
    : _columnAliases(new ParseAliasMap()),
      _tableAliases(new ParseAliasMap()),
      _slFactory(new SelectListFactory(_columnAliases)),
      _fFactory(new FromFactory(_tableAliases)),
      _wFactory(new WhereFactory()) {
    
}

void
SelectFactory::attachTo(SqlSQL2Parser& p) {
    _slFactory->attachTo(p);
    _fFactory->attachTo(p);
    _wFactory->attachTo(p);
}
 

////////////////////////////////////////////////////////////////////////
// SelectListFactory
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

void
SelectListFactory::attachTo(SqlSQL2Parser& p) {
    _selectListH.reset(new SelectListH(*this));
    _columnAliasH.reset(new ColumnAliasH(_aliases));
    p._selectListHandler = _selectListH;    
}

void
SelectListFactory::_import(RefAST selectRoot) {

}

