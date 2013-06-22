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
  * @file SelectList.cc
  *
  * @brief Implementation of a SelectList
  *
  * @author Daniel L. Wang, SLAC
  */
// SelectList design notes:
// Idea was to have this as an intermediate query tree representation.
// This might be practical through the use of factories to hide enough
// of the ANTLR-specific parts. Because we have inserted nodes in the
// ANTLR tree, node navigation should be sensible enough that the
// ANTLR-specific complexity can be minimized to only a dependence on
// the tree node structure.

// Should we keep a hash table when column refs are detected, so we can
// map them?
// For now, just build the syntax tree without evaluating.
#include "lsst/qserv/master/SelectList.h"
#include "lsst/qserv/master/FuncExpr.h"
#include "lsst/qserv/master/ValueFactor.h"
#include "lsst/qserv/master/QueryTemplate.h"
#include <iterator>

#include "SqlSQL2TokenTypes.hpp" // For ANTLR typing.

using lsst::qserv::master::ColumnRef;
using lsst::qserv::master::ColumnRefList;
using lsst::qserv::master::ValueExpr;
using lsst::qserv::master::ValueExprPtr;
using lsst::qserv::master::SelectList;
namespace qMaster=lsst::qserv::master;

namespace { // File-scope helpers
template <typename T>
struct renderWithSep {
    renderWithSep(qMaster::QueryTemplate& qt_, std::string const& sep_) 
        : qt(qt_),sep(sep_),count(0) {}
    void operator()(T const& t) {
        if(++count > 1) qt.append(sep);
    }
    qMaster::QueryTemplate& qt;
    std::string sep;
    int count;

};
} // anonymous namespace
void
SelectList::addStar(antlr::RefAST table) {
    assert(_valueExprList.get());

    ValueExprPtr ve;
    std::string tParam;
    if(table.get()) {
        tParam = qMaster::tokenText(table);
    }
    ve = ValueExpr::newSimple(ValueFactor::newStarFactor(tParam));
    _valueExprList->push_back(ve);
}

void
SelectList::dbgPrint() const {
    assert(_valueExprList.get());
    std::cout << "Parsed value expression for select list." << std::endl;
    std::copy(_valueExprList->begin(),
              _valueExprList->end(),
              std::ostream_iterator<ValueExprPtr>(std::cout, "\n"));    
}

std::ostream& 
qMaster::operator<<(std::ostream& os, qMaster::SelectList const& sl) {
    os << "SELECT ";
    std::copy(sl._valueExprList->begin(), sl._valueExprList->end(),
                  std::ostream_iterator<ValueExprPtr>(os,", "));
    os << "(FIXME)";
    return os;
}

std::string
qMaster::SelectList::getGenerated() {
    QueryTemplate qt;
    renderTo(qt);
    return qt.dbgStr();
}

void
qMaster::SelectList::renderTo(qMaster::QueryTemplate& qt) const {
    std::for_each(_valueExprList->begin(), _valueExprList->end(),
                  ValueExpr::render(qt, true));

}
struct copyValueExpr {
    qMaster::ValueExprPtr operator()(ValueExprPtr const& p) {
        return p->clone();
    }
};
boost::shared_ptr<SelectList> qMaster::SelectList::copyDeep() {
    boost::shared_ptr<SelectList> newS(new SelectList(*this));
    newS->_valueExprList.reset(new ValueExprList());
    ValueExprList& src = *_valueExprList;
    std::transform(src.begin(), src.end(), 
                   std::back_inserter(*newS->_valueExprList),
                   // std::mem_fun(&ValueExpr::clone));
                   copyValueExpr());

    // For the other fields, default-copied versions are okay.
    return newS;
}

boost::shared_ptr<SelectList> qMaster::SelectList::copySyntax() {
    boost::shared_ptr<SelectList> newS(new SelectList(*this));
    // Shallow copy of expr list is okay.
    newS->_valueExprList.reset(new ValueExprList(*_valueExprList));
    // For the other fields, default-copied versions are okay.
    return newS;
}
