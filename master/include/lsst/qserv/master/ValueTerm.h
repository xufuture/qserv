// -*- LSST-C++ -*-
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
// ValueTerm is a term in a ValueExpr's "term (term_op term)*" phrase
// This needs to be reconciled with the WhereClause's ValueExprTerm

#ifndef LSST_QSERV_MASTER_VALUEEXPR_H
#define LSST_QSERV_MASTER_VALUEEXPR_H

#include <list>
#include <string>
#include <boost/shared_ptr.hpp>

namespace lsst { namespace qserv { namespace master {
// Forward
class ColumnRef;
class QueryTemplate;
class FuncExpr;

class ValueTerm; 
typedef boost::shared_ptr<ValueTerm> ValueTermPtr;
typedef std::list<ValueTermPtr> ValueTermList;

class ValueTerm {
public:
    enum Type { COLUMNREF, FUNCTION, AGGFUNC, STAR, CONST };

    // May need non-const, otherwise, need new construction
    boost::shared_ptr<ColumnRef const> getColumnRef() const { return _columnRef; }
    boost::shared_ptr<ColumnRef> getColumnRef() { return _columnRef; }
    boost::shared_ptr<FuncExpr const> getFuncExpr() const { return _funcExpr; }
    boost::shared_ptr<FuncExpr> getFuncExpr() { return _funcExpr; }
    Type getType() const { return _type; }
    std::string const& getAlias() const { return _alias; }
    void setAlias(std::string const& a) { _alias = a; }
    std::string const& getTableStar() const { return _tableStar; }
    void setTableStar(std::string const& a) { _tableStar = a; }

    ValueTermPtr clone() const;

    static ValueTermPtr newColumnRefTerm(boost::shared_ptr<ColumnRef const> cr);
    static ValueTermPtr newStarTerm(std::string const& table);
    static ValueTermPtr newAggTerm(boost::shared_ptr<FuncExpr> fe);
    static ValueTermPtr newFuncTerm(boost::shared_ptr<FuncExpr> fe);
    static ValueTermPtr newConstTerm(std::string const& alnum);
    friend std::ostream& operator<<(std::ostream& os, ValueTerm const& ve);
    friend std::ostream& operator<<(std::ostream& os, ValueTerm const* ve);

    class render;
    friend class render;
private:
    Type _type;
    boost::shared_ptr<ColumnRef> _columnRef;
    boost::shared_ptr<FuncExpr> _funcExpr;
    std::string _alias;
    std::string _tableStar; // Reused as const val (no tablestar)
};

class ValueTerm::render : public std::unary_function<ValueTerm, void> {
public:
    render(QueryTemplate& qt) : _qt(qt) {}
    void operator()(ValueTerm const& ve);
    void operator()(ValueTerm const* vep) { 
        if(vep) (*this)(*vep); }
    void operator()(boost::shared_ptr<ValueTerm> const& vep) { 
        (*this)(vep.get()); }
    QueryTemplate& _qt;
};


}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_VALUEEXPR_H
