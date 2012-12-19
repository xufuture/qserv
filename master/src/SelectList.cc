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
// SelectList

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
#include "lsst/qserv/master/QueryTemplate.h"
#include <iterator>
#include <boost/make_shared.hpp>

#include "SqlSQL2TokenTypes.hpp" // For ANTLR typing.

using lsst::qserv::master::ColumnRef;
using lsst::qserv::master::ColumnRefList;
using lsst::qserv::master::ValueExpr;
using lsst::qserv::master::ValueExprPtr;
using lsst::qserv::master::SelectList;
using lsst::qserv::master::FromList;
using lsst::qserv::master::OrderByClause;
using lsst::qserv::master::HavingClause;
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
ColumnRefList::acceptColumnRef(antlr::RefAST d, antlr::RefAST t, 
                               antlr::RefAST c) {
    using lsst::qserv::master::tokenText;
    boost::shared_ptr<ColumnRef> cr(new ColumnRef(tokenText(d), 
                                                  tokenText(t), 
                                                  tokenText(c)));
    antlr::RefAST first = d;
    if(!d.get()) { 
        if (!t.get()) { first = c; }
        else first = t; 
    } 
    _refs[first] = cr;
    // Don't add to list. Let selectList handle it later. Only track for now.
    // Need to be able to lookup ref by RefAST.
}

void
SelectList::addStar(antlr::RefAST table) {
    assert(_valueExprList.get());

    ValueExprPtr ve;
    if(table.get()) {
        ve = ValueExpr::newStarExpr(qMaster::tokenText(table));
    } else {
        ve = ValueExpr::newStarExpr(std::string());
    }
    _valueExprList->push_back(ve);
}

void
SelectList::addFunc(antlr::RefAST a) {
    assert(_valueExprList.get());
   boost::shared_ptr<FuncExpr> fe(new FuncExpr());
   if(a->getType() == SqlSQL2TokenTypes::FUNCTION_SPEC) { a = a->getFirstChild(); }
   //std::cout << "fspec name:" << tokenText(a) << std::endl;
   fe->name = tokenText(a);
   _fillParams(fe->params, a->getNextSibling());
   _valueExprList->push_back(ValueExpr::newFuncExpr(fe));
}

void
SelectList::addAgg(antlr::RefAST a) {
    assert(_valueExprList.get());
   boost::shared_ptr<FuncExpr> fe(new FuncExpr());
   fe->name = tokenText(a);
   _fillParams(fe->params, a->getFirstChild());
   _valueExprList->push_back(ValueExpr::newAggExpr(fe));
}

void
SelectList::addRegular(antlr::RefAST a) {
    assert(_valueExprList.get());
    boost::shared_ptr<ColumnRef const> cr(_columnRefList->getRef(a));
    _valueExprList->push_back(ValueExpr::newColumnRefExpr(cr));
}

void
SelectList::dbgPrint() const {
    assert(_valueExprList.get());
    std::cout << "Parsed value expression for select list." << std::endl;
    std::copy(_valueExprList->begin(),
              _valueExprList->end(),
              std::ostream_iterator<ValueExprPtr>(std::cout, "\n"));

    
}
ValueExprPtr _newColumnRef(antlr::RefAST v) {
    ValueExprPtr e(new ValueExpr());
    e->_type = ValueExpr::COLUMNREF;
    e->_columnRef.reset(new qMaster::ColumnRef("","",qMaster::walkSiblingString(v) + "FIXME"));
    //std::cout << "need to make column ref out of " << qMaster::tokenText(v) << std::endl;
    return e;
}

ValueExprPtr _newValueExpr(antlr::RefAST v) {
    ValueExprPtr e(new ValueExpr());
    // Figure out what type of value expr, and create it properly.
    std::cout << "Type of:" << v->getText() << "(" << v->getType() << ")" << std::endl;
    switch(v->getType()) {
    case SqlSQL2TokenTypes::ASTERISK:
        std::cout << "star*: " << std::endl;
        return ValueExpr::newStarExpr(std::string());
    case SqlSQL2TokenTypes::VALUE_EXP:
        v = v->getFirstChild();
        switch(v->getType()) {
        case SqlSQL2TokenTypes::REGULAR_ID:
            std::cout << "Regular id: " << qMaster::tokenText(v) << std::endl;
            return  _newColumnRef(v);
            // antlr::RefAST a = _aliasMap->getAlias(v);
            // if(a.get()) ve->_alias = qMaster::tokenText(a);
            break;
        case SqlSQL2TokenTypes::FUNCTION_SPEC:
            // FIXME.
            std::cout << "nested function. FIXME. Nesting not supported" << std::endl;
        };

        std::cout << "ValueExp child:" << v->getText() << "(" << v->getType() << ")" << std::endl;
        break;
    default: 
            break;
    };
    return e;
}

void 
SelectList::_fillParams(ValueExprList& p, antlr::RefAST pnodes) {
    antlr::RefAST current = pnodes;
    //std::cout << "params got " << tokenText(pnodes) << std::endl;
    // Make sure the parser gave us the right nodes.
    assert(current->getType() == SqlSQL2TokenTypes::LEFT_PAREN); 
    for(current = current->getNextSibling(); 
        current.get(); 
        current=current->getNextSibling()) {
        if(current->getType() == SqlSQL2TokenTypes::COMMA) { continue; }
        if(current->getType() == SqlSQL2TokenTypes::RIGHT_PAREN) { break; }
        ValueExprPtr ve = _newValueExpr(current);
        if(!ve.get()) {
            throw std::string("Qserv internal error: Couldn't build valueExpr from " 
                              + tokenText(current));
        }
        p.push_back(ve);
    }
    //std::cout << "printing params \n";
    //printIndented(pnodes);
    // FIXME
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
                  ValueExpr::render(qt));

}

boost::shared_ptr<SelectList> qMaster::SelectList::copySyntax() {
    boost::shared_ptr<SelectList> newS(new SelectList(*this));
    // Shallow copy of expr list is okay.
    newS->_valueExprList.reset(new ValueExprList(*_valueExprList));
    // For the other fields, default-copied versions are okay.
    return newS;
}
////////////////////////////////////////////////////////////////////////
// FromList
////////////////////////////////////////////////////////////////////////
std::ostream& 
qMaster::operator<<(std::ostream& os, qMaster::FromList const& fl) {
    os << "FROM ";
    if(fl._tableRefns.get() && fl._tableRefns->size() > 0) {
        TableRefnList const& refList = *(fl._tableRefns);
        std::copy(refList.begin(), refList.end(),
                  std::ostream_iterator<TableRefN::Ptr>(os,", "));
    } else {
        os << "(empty)";
    }
    return os;
}

std::string
qMaster::FromList::getGenerated() {
    QueryTemplate qt;
    renderTo(qt);
    return qt.dbgStr();
}

void
qMaster::FromList::renderTo(qMaster::QueryTemplate& qt) const {
    if(_tableRefns.get() && _tableRefns->size() > 0) {
        TableRefnList const& refList = *_tableRefns;
        std::for_each(refList.begin(), refList.end(), TableRefN::render(qt));
    } 
}

boost::shared_ptr<qMaster::FromList> qMaster::FromList::copySyntax() {
    boost::shared_ptr<FromList> newL(new FromList(*this));
    // Shallow copy of expr list is okay.
    newL->_tableRefns.reset(new TableRefnList(*_tableRefns));
    // For the other fields, default-copied versions are okay.
    return newL;
}
////////////////////////////////////////////////////////////////////////
// OrderByTerm
////////////////////////////////////////////////////////////////////////
std::ostream& 
qMaster::operator<<(std::ostream& os, qMaster::OrderByTerm const& t) {
    os << *(t._expr);
    if(!t._collate.empty()) os << " COLLATE " << t._collate;
    switch(t._order) {
    case qMaster::OrderByTerm::ASC: os << " ASC"; break;
    case qMaster::OrderByTerm::DESC: os << " DESC"; break;
    case qMaster::OrderByTerm::DEFAULT: break;
    }
    return os;
}
////////////////////////////////////////////////////////////////////////
// OrderByClause
////////////////////////////////////////////////////////////////////////
std::ostream& 
qMaster::operator<<(std::ostream& os, qMaster::OrderByClause const& c) {
    if(c._terms.get()) {
        os << "ORDER BY ";
        std::copy(c._terms->begin(),c._terms->end(),
                  std::ostream_iterator<qMaster::OrderByTerm>(os,", "));
    }
    return os;
}

std::string
qMaster::OrderByClause::getGenerated() {
    QueryTemplate qt;
    renderTo(qt);
    return qt.dbgStr();
}

void
qMaster::OrderByClause::renderTo(qMaster::QueryTemplate& qt) const {
    std::stringstream ss;
    if(_terms.get()) {
        std::copy(_terms->begin(), _terms->end(),
                  std::ostream_iterator<qMaster::OrderByTerm>(ss,", "));
    }
    qt.append(ss.str());
    // FIXME
}
boost::shared_ptr<OrderByClause> OrderByClause::copySyntax() {
    return boost::make_shared<OrderByClause>(*this);
}
////////////////////////////////////////////////////////////////////////
// HavingClause
////////////////////////////////////////////////////////////////////////
std::ostream& 
qMaster::operator<<(std::ostream& os, qMaster::HavingClause const& c) {
    if(!c._expr.empty()) {
        os << "HAVING " << c._expr;
    }
    return os;
}
std::string
qMaster::HavingClause::getGenerated() {
    QueryTemplate qt;
    renderTo(qt);
    return qt.dbgStr();
}
void
qMaster::HavingClause::renderTo(qMaster::QueryTemplate& qt) const {
    if(!_expr.empty()) {
        qt.append(_expr);
    }
}

boost::shared_ptr<HavingClause> HavingClause::copySyntax() {
    return boost::make_shared<HavingClause>(*this);
}
