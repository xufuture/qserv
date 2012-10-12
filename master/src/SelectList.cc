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
std::ostream& output(std::ostream& os, qMaster::ValueExprList const& vel) {
    std::copy(vel.begin(), vel.end(),
              std::ostream_iterator<qMaster::ValueExprPtr>(os, ";"));
    
    return os;
}
void renderList(qMaster::QueryTemplate& qt, qMaster::ValueExprList const& vel) {
    qt.pushDelim();
    std::for_each(vel.begin(), vel.end(), ValueExpr::render(qt));
    qt.popDelim();
}
} // anonymous namespace


std::ostream& qMaster::operator<<(std::ostream& os, qMaster::ColumnRef const& cr) {
    return os << "(" << cr.db << "," << cr.table << "," << cr.column << ")";
}
std::ostream& qMaster::operator<<(std::ostream& os, qMaster::ColumnRef const* cr) {
    return os << *cr;
}

void qMaster::ColumnRef::render(QueryTemplate& qt) const {
    qt.pushDelim();
    if(!db.empty()) { qt.append(db); qt.append("."); }
    if(!table.empty()) {qt.append(table); qt.append("."); }
    qt.append(column);
    qt.popDelim();
}

std::ostream& qMaster::operator<<(std::ostream& os, FuncExpr const& fe) {
    os << "(" << fe.name << ",";
    output(os, fe.params);
    os << ")";
    return os;
}
std::ostream& qMaster::operator<<(std::ostream& os, FuncExpr const* fe) {
    return os << *fe;
}

void qMaster::FuncExpr::render(qMaster::QueryTemplate& qt) const {
    qt.pushDelim();
    qt.append(name); 
    qt.append("(");
    renderList(qt, params);
    qt.append(")");
    qt.popDelim();
}


ValueExprPtr ValueExpr::newColumnRefExpr(boost::shared_ptr<ColumnRef const> cr) {
    ValueExprPtr expr(new ValueExpr());
    expr->_type = COLUMNREF;
    expr->_columnRef = cr;
    return expr;
}

ValueExprPtr ValueExpr::newStarExpr(std::string const& table) {
    ValueExprPtr expr(new ValueExpr());
    expr->_type = STAR;
    if(!table.empty()) {
        expr->_tableStar = table;
    }
    return expr;
}
ValueExprPtr ValueExpr::newFuncExpr(boost::shared_ptr<FuncExpr> fe) {
    ValueExprPtr expr(new ValueExpr());
    expr->_type = FUNCTION;
    expr->_funcExpr = fe;
    return expr;
}

ValueExprPtr ValueExpr::newAggExpr(boost::shared_ptr<FuncExpr> fe) {
    ValueExprPtr expr(new ValueExpr());
    expr->_type = AGGFUNC;
    expr->_funcExpr = fe;
    return expr;
}

std::ostream& qMaster::operator<<(std::ostream& os, ValueExpr const& ve) {
    switch(ve._type) {
    case ValueExpr::COLUMNREF: os << "CREF: " << *(ve._columnRef); break;
    case ValueExpr::FUNCTION: os << "FUNC: " << *(ve._funcExpr); break;
    case ValueExpr::AGGFUNC: os << "AGGFUNC: " << *(ve._funcExpr); break;
    case ValueExpr::STAR: 
        os << "<";
        if(!ve._tableStar.empty()) os << ve._tableStar << ".";
        os << "*>"; 
        break;
    default: os << "UnknownExpr"; break;
    }
    if(!ve._alias.empty()) { os << " [" << ve._alias << "]"; }
    return os;
}

std::ostream& qMaster::operator<<(std::ostream& os, ValueExpr const* ve) {
    if(!ve) return os << "<NULL>";
    return os << *ve;
}

void qMaster::ValueExpr::render::operator()(qMaster::ValueExpr const& ve) {
    std::stringstream ss;
    if(_count++ > 0) _qt.append(",");
    switch(ve._type) {
    case ValueExpr::COLUMNREF: ve._columnRef->render(_qt); break;
    case ValueExpr::FUNCTION: ve._funcExpr->render(_qt); break;
    case ValueExpr::AGGFUNC: ve._funcExpr->render(_qt); break;
    case ValueExpr::STAR: 
        if(!ve._tableStar.empty()) ss << ve._tableStar << ".";
        ss << "*"; 
        _qt.append(ss.str());
        break;
    default: break;
    }
    if(!ve._alias.empty()) { _qt.append("AS"); _qt.append(ve._alias); }
    _qt.pushDelim();
    _qt.popDelim();
}

std::string qMaster::QueryTemplate::dbgStr() const {
    std::stringstream ss;
    std::copy(_elements.begin(), _elements.end(),
              std::ostream_iterator<std::string>(ss, ""));
    return ss.str();
}

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
    std::stringstream ss;
    std::for_each(_valueExprList->begin(), _valueExprList->end(),
                  ValueExpr::render(qt));
    return qt.dbgStr();
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
qMaster::FromList::getGenerated(){
    QueryTemplate qt;
    std::stringstream ss;
    if(_tableRefns.get() && _tableRefns->size() > 0) {
        TableRefnList const& refList = *_tableRefns;
        std::for_each(refList.begin(), refList.end(), TableRefN::render(qt));
    } 
    return qt.dbgStr();
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
    std::stringstream ss;
    if(_terms.get()) {
        std::copy(_terms->begin(), _terms->end(),
                  std::ostream_iterator<qMaster::OrderByTerm>(ss,", "));
    }
    return ss.str();
}
////////////////////////////////////////////////////////////////////////
// GroupByTerm
////////////////////////////////////////////////////////////////////////
std::ostream& 
qMaster::operator<<(std::ostream& os, qMaster::GroupByTerm const& t) {
    os << *(t._expr);
    if(!t._collate.empty()) os << " COLLATE " << t._collate;
    return os;
}
////////////////////////////////////////////////////////////////////////
// GroupByClause
////////////////////////////////////////////////////////////////////////
std::ostream& 
qMaster::operator<<(std::ostream& os, qMaster::GroupByClause const& c) {
    if(c._terms.get()) {
        os << "GROUP BY ";
        std::copy(c._terms->begin(),c._terms->end(),
              std::ostream_iterator<qMaster::GroupByTerm>(os,", "));
    }
    return os;
}
std::string
qMaster::GroupByClause::getGenerated() {
    std::stringstream ss;
    if(_terms.get()) {
        std::copy(_terms->begin(), _terms->end(),
              std::ostream_iterator<qMaster::GroupByTerm>(ss,", "));
    }
    return ss.str();
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
    if(!_expr.empty()) {
        return _expr;
    }
    return std::string();
}
