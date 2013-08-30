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
/**
  * @file SelectFactory.cc
  *
  * @brief Implementation of the SelectFactory, which is responsible
  * (through some delegated behavior) for constructing SelectStmt (and
  * SelectList, etc.) from an ANTLR parse tree.
  *
  * Includes parse handlers: SelectListH, SelectStarH, ColumnAliasH
  *
  * @author Daniel L. Wang, SLAC
  */
#include "lsst/qserv/master/SelectFactory.h"

// antlr
#include "SqlSQL2Parser.hpp" // applies several "using antlr::***".

// Package
#include "lsst/qserv/master/ColumnRefH.h" // ColumnRefNodeMap
#include "lsst/qserv/master/ParseAliasMap.h"
#include "lsst/qserv/master/SelectStmt.h"

// Delegate factories
#include "lsst/qserv/master/FromFactory.h"
#include "lsst/qserv/master/ModFactory.h"
#include "lsst/qserv/master/SelectListFactory.h"
#include "lsst/qserv/master/ValueExprFactory.h"
#include "lsst/qserv/master/WhereFactory.h"

////////////////////////////////////////////////////////////////////////
// SelectFactory
////////////////////////////////////////////////////////////////////////
namespace lsst {
namespace qserv {
namespace master {

SelectFactory::SelectFactory()
    : _columnAliases(new ParseAliasMap()),
      _tableAliases(new ParseAliasMap()),
      _columnRefNodeMap(new ColumnRefNodeMap()),
      _vFactory(new ValueExprFactory(_columnRefNodeMap)) {

    _fFactory.reset(new FromFactory(_tableAliases, _vFactory));
    _slFactory.reset(new SelectListFactory(_columnAliases, _vFactory));
    _mFactory.reset(new ModFactory(_vFactory));
    _wFactory.reset(new WhereFactory(_vFactory));
}

void
SelectFactory::attachTo(SqlSQL2Parser& p) {
    _attachShared(p);

    _slFactory->attachTo(p);
    _fFactory->attachTo(p);
    _wFactory->attachTo(p);
    _mFactory->attachTo(p);
}

boost::shared_ptr<SelectStmt>
SelectFactory::getStatement() {
    boost::shared_ptr<SelectStmt> stmt(new SelectStmt());
    stmt->_selectList = _slFactory->getProduct();
    stmt->_fromList = _fFactory->getProduct();
    stmt->_whereClause = _wFactory->getProduct();
    stmt->_orderBy = _mFactory->getOrderBy();
    stmt->_groupBy = _mFactory->getGroupBy();
    stmt->_having = _mFactory->getHaving();
    stmt->_limit = _mFactory->getLimit();
    return stmt;
}

void
SelectFactory::_attachShared(SqlSQL2Parser& p) {
    boost::shared_ptr<ColumnRefH> crh(new ColumnRefH());
    crh->setListener(_columnRefNodeMap);
    p._columnRefHandler = crh;
}

}}} // lsst::qserv::master
