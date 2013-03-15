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
// WhereClause.cc houses the implementation of a WhereClause, a
// container for the parsed elements of a SQL WHERE.
#include "lsst/qserv/master/WhereClause.h"

#include <iostream>
#include "lsst/qserv/master/QueryTemplate.h"

namespace qMaster=lsst::qserv::master;
using lsst::qserv::master::QsRestrictor;
using lsst::qserv::master::WhereClause;

namespace { // File-scope helpers
}


////////////////////////////////////////////////////////////////////////
// QsRestirctor::render
////////////////////////////////////////////////////////////////////////
void QsRestrictor::render::operator()(QsRestrictor::Ptr const& p) {
    if(p.get()) {
        qt.append(p->_name);
        qt.append("(");
        StringList::const_iterator i;
        int c=0;
        for(i=p->_params.begin(); i != p->_params.end(); ++i) {
            if(++c > 1) qt.append(",");
            qt.append(*i);
        }
        qt.append(")");
    }
}


////////////////////////////////////////////////////////////////////////
// WhereClause
////////////////////////////////////////////////////////////////////////
std::ostream& 
qMaster::operator<<(std::ostream& os, WhereClause const& wc) {
    os << "WHERE " << wc._original;
    return os;
}

std::string
WhereClause::getGenerated() {
    QueryTemplate qt;
    renderTo(qt);
    return qt.dbgStr();
}
void WhereClause::renderTo(QueryTemplate& qt) const {
    if(_restrs.get()) {
        std::for_each(_restrs->begin(), _restrs->end(), 
                      QsRestrictor::render(qt));
    }
    if(_tree.get()) {
        _tree->renderTo(qt);
    }
}

boost::shared_ptr<WhereClause> WhereClause::copyDeep() const {
    // FIXME
    boost::shared_ptr<WhereClause> newC(new WhereClause(*this));
    // Shallow copy of expr list is okay.
    if(_tree.get()) {
        newC->_tree = _tree->copySyntax();
    }
    // For the other fields, default-copied versions are okay.
    return newC;

}

boost::shared_ptr<WhereClause> WhereClause::copySyntax() {
    boost::shared_ptr<WhereClause> newC(new WhereClause(*this));
    // Shallow copy of expr list is okay.
    if(_tree.get()) {
        newC->_tree = _tree->copySyntax();
    }
    // For the other fields, default-copied versions are okay.
    return newC;
}

////////////////////////////////////////////////////////////////////////
// WhereClause (private)
////////////////////////////////////////////////////////////////////////
void
WhereClause::_resetRestrs() {
    _restrs.reset(new QsRestrictor::List());
}

////////////////////////////////////////////////////////////////////////
// BoolTerm section
////////////////////////////////////////////////////////////////////////
std::ostream& qMaster::OrTerm::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& qMaster::AndTerm::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& qMaster::BoolFactor::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& qMaster::UnknownTerm::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& qMaster::PassTerm::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
std::ostream& qMaster::ValueExprTerm::putStream(std::ostream& os) const {
    // FIXME
    return os;
}
class qMaster::BoolTerm::render {
public:
    render(QueryTemplate& qt_) : qt(qt_) {}
    void operator()(BoolTerm::Ptr const& t) {
        t->renderTo(qt);
    }
    QueryTemplate& qt;
};
namespace {
template <typename Plist>
inline void renderList(qMaster::QueryTemplate& qt, 
                       Plist const& lst, 
                       std::string const& sep) {
    int count=0;
    typename Plist::const_iterator i;
    for(i = lst.begin(); i != lst.end(); ++i) {
        if(!sep.empty() && ++count > 1) { qt.append(sep); }
        assert(i->get());
        (**i).renderTo(qt);
    }
}
}
void qMaster::OrTerm::renderTo(QueryTemplate& qt) const {
    renderList(qt, _terms, "OR");
}
void qMaster::AndTerm::renderTo(QueryTemplate& qt) const {
    renderList(qt, _terms, "AND");
}
void qMaster::BoolFactor::renderTo(QueryTemplate& qt) const {
    std::string s;
    renderList(qt, _terms, s);
}
void qMaster::UnknownTerm::renderTo(QueryTemplate& qt) const {
    qt.append("unknown");
}
void qMaster::PassTerm::renderTo(QueryTemplate& qt) const {
    qt.append(_text);
}
void qMaster::ValueExprTerm::renderTo(QueryTemplate& qt) const {
    ValueExpr::render r(qt);
    r(_expr);
    assert(_expr.get());
}

boost::shared_ptr<qMaster::BoolTerm> qMaster::OrTerm::copySyntax() {
    boost::shared_ptr<OrTerm> ot(new OrTerm());
    ot->_terms = _terms; // shallow copy for now
    return ot;
}
boost::shared_ptr<qMaster::BoolTerm> qMaster::AndTerm::copySyntax() {
    boost::shared_ptr<AndTerm> at(new AndTerm());
    at->_terms = _terms; // shallow copy for now
    return at;
}
