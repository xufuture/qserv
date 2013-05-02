// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
// ValueExpr is a parse element commonly found in SelectList elements.

#ifndef LSST_QSERV_MASTER_VALUETERM_H
#define LSST_QSERV_MASTER_VALUETERM_H

#include <iostream>
#include <list>
#include <string>
#include <boost/shared_ptr.hpp>

namespace lsst { namespace qserv { namespace master {
// Forward
class QueryTemplate;

class ValueExpr; 
typedef boost::shared_ptr<ValueExpr> ValueExprPtr;
typedef std::list<ValueExprPtr> ValueExprList;
class ValueTerm; 

class ValueExpr {
public:
    enum Op {NONE=200, UNKNOWN, PLUS, MINUS};
    struct TermOp {
        boost::shared_ptr<ValueTerm> term;
        Op op;
    };
    typedef std::list<TermOp> TermOpList;

    std::string const& getAlias() const { return _alias; }
    void setAlias(std::string const& a) { _alias = a; }

#if 0
    boost::shared_ptr<ValueTerm> getTerm() { return _term; }
    boost::shared_ptr<ValueTerm const> getTerm() const { return _term; }
    void setTerm(boost::shared_ptr<ValueTerm> t) { _term = t; }

    Op getOp() const { return _op; }
    void setOp(Op op) { _op = op; }
#endif

    TermOpList& getTermOps() { return _termOps; }
    TermOpList const& getTermOps() const { return _termOps; }

    ValueExprPtr clone() const;
    friend std::ostream& operator<<(std::ostream& os, ValueExpr const& vt);
    friend std::ostream& operator<<(std::ostream& os, ValueExpr const* vt);

    static ValueExprPtr newSimple(boost::shared_ptr<ValueTerm> vt);
   
#if 0
    // Convenience navigators
    template <class F> 
    class copyTransformExpr {
    public:
        copyTransformExpr(F& f) : _f(f) {}
        void acceptTerm(ValueTerm const* vtp) {
            currentVe.reset(new ValueExpr);
            if(parent.get()) { // attach to parent.
                parent->_next = currentVe;
            } else { // No parent, so set the root.
                root = currentVe;
            }
            currentVe->_term = _f(*vtp);
        }
        void acceptOp(Op op) { currentVe->_op = op; }
        F& _f;
        boost::shared_ptr<ValueExpr> root;
        boost::shared_ptr<ValueExpr> parent;
        boost::shared_ptr<ValueExpr> currentVe;
    };

    template <class F>
    void mapTerms(F& f) const {
        for(ValueExpr const* ep = this;
            ep;
            ep = ep->getNext().get()) {
            f.acceptTerm(ep->getTerm().get());
            f.acceptOp(ep->getOp());
        }
    }
#endif

    friend class ValueExprFactory;
    class render;
    friend class render;
private:
    ValueExpr();
    std::string _alias;
    std::list<TermOp> _termOps;
};

class ValueExpr::render : public std::unary_function<ValueExpr, void> {
public:
    render(QueryTemplate& qt, bool needsComma) 
        : _qt(qt), _needsComma(needsComma), _count(0) {}
    void operator()(ValueExpr const& ve);
    void operator()(ValueExpr const* vep) { 
        if(vep) (*this)(*vep); }
    void operator()(boost::shared_ptr<ValueExpr> const& vep) { 
        (*this)(vep.get()); }
    QueryTemplate& _qt;
    bool _needsComma;
    int _count;
};


}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_VALUEEXPR_H
