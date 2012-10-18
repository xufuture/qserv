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
// ColumnRef is a representation of a parsed column reference in SQL.

#ifndef LSST_QSERV_MASTER_VALUEEXPR_H
#define LSST_QSERV_MASTER_VALUEEXPR_H

#include <list>
#include <string>
#include <boost/shared_ptr.hpp>

namespace lsst { namespace qserv { namespace master {
// Forward
class ColumnRef;
class QueryTemplate;
class ValueExpr; 

typedef boost::shared_ptr<ValueExpr> ValueExprPtr;
typedef std::list<ValueExprPtr> ValueExprList;

class FuncExpr {
public:
    std::string getName() const;
    ValueExprList getParams() const;

    std::string name;
    ValueExprList params;
    friend std::ostream& operator<<(std::ostream& os, FuncExpr const& fe);
    friend std::ostream& operator<<(std::ostream& os, FuncExpr const* fe);
    void render(QueryTemplate& qt) const;
};

typedef boost::shared_ptr<FuncExpr> FuncExprPtr;

class ValueExpr {
public:
    enum Type { COLUMNREF, FUNCTION, AGGFUNC, STAR, CONST };

    boost::shared_ptr<ColumnRef const> getColumnRef() const { return _columnRef; }
    boost::shared_ptr<FuncExpr> getFuncExpr() const { return _funcExpr; }
    Type getType() const { return _type; }
    
    static ValueExprPtr newColumnRefExpr(boost::shared_ptr<ColumnRef const> cr);
    static ValueExprPtr newStarExpr(std::string const& table);
    static ValueExprPtr newAggExpr(boost::shared_ptr<FuncExpr> fe);
    static ValueExprPtr newFuncExpr(boost::shared_ptr<FuncExpr> fe);
    friend std::ostream& operator<<(std::ostream& os, ValueExpr const& ve);
    friend std::ostream& operator<<(std::ostream& os, ValueExpr const* ve);

    class render;
    friend class render;
    //private:
    Type _type;
    boost::shared_ptr<ColumnRef const> _columnRef;
    boost::shared_ptr<FuncExpr> _funcExpr;
    std::string _alias;
    std::string _tableStar; // Reused as const val (no tablestar)
};

class ValueExpr::render : public std::unary_function<ValueExpr, void> {
public:
    render(QueryTemplate& qt) : _qt(qt), _count(0) {}
    void operator()(ValueExpr const& ve);
    void operator()(ValueExpr const* vep) { 
        if(vep) (*this)(*vep); }
    void operator()(boost::shared_ptr<ValueExpr> const& vep) { 
        (*this)(vep.get()); }
    QueryTemplate& _qt;
    int _count;
};


}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_VALUEEXPR_H
