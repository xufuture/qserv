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
// SelectStmt contains extracted information about a particular parsed
// SQL select statement. It is not responsible for performing
// verification, validation, or other processing that requires
// persistent or run-time state.
#ifndef LSST_QSERV_MASTER_SELECTLIST_H
#define LSST_QSERV_MASTER_SELECTLIST_H

#include <list>
#include <map>
#include <deque>
#include <antlr/AST.hpp>
#include <boost/shared_ptr.hpp>

#include "lsst/qserv/master/ColumnRef.h"
#include "lsst/qserv/master/ColumnRefList.h"
#include "lsst/qserv/master/TableRefN.h"
#include "lsst/qserv/master/ValueExpr.h"

namespace lsst {
namespace qserv {
namespace master {
// Forward
class ColumnRefMap;
class ColumnAliasMap;
class QueryTemplate;
class BoolTerm;
class GroupByClause;

class SelectList {
public:
    SelectList() 
        : _columnRefList(new ColumnRefList()) ,
          _valueExprList(new ValueExprList())
    {
        _columnRefList->setValueExprList(_valueExprList);
    }
    ~SelectList() {}
    boost::shared_ptr<ColumnRefList> getColumnRefList() {
        return _columnRefList;
    }

    void addStar(antlr::RefAST table);
    void addRegular(antlr::RefAST n);
    void addFunc(antlr::RefAST n);
    void addAgg(antlr::RefAST n);
    
    void dbgPrint() const;
    std::string getGenerated();
    void renderTo(QueryTemplate& qt) const;
    boost::shared_ptr<SelectList> copySyntax();

    // non-const accessor for query manipulation.
    boost::shared_ptr<ValueExprList> getValueExprList() 
        { return _valueExprList; }

    friend class SelectListFactory;
private:
    friend std::ostream& operator<<(std::ostream& os, SelectList const& sl);
    void _fillParams(ValueExprList& p, antlr::RefAST pnodes);
    boost::shared_ptr<ColumnRefList> _columnRefList;
    boost::shared_ptr<ValueExprList> _valueExprList;
    boost::shared_ptr<ColumnRefMap const> _aliasMap;
};

class OrderByTerm {
public:
    enum Order {DEFAULT, ASC, DESC};
        
    OrderByTerm() {}
    OrderByTerm(boost::shared_ptr<ValueExpr> val,
                Order _order,
                std::string _collate);

    ~OrderByTerm() {}

    boost::shared_ptr<const ValueExpr> getExpr();
    Order getOrder() const;
    std::string getCollate() const;

private:
    friend std::ostream& operator<<(std::ostream& os, OrderByTerm const& ob);
    friend class ModFactory;

    boost::shared_ptr<ValueExpr> _expr;
    Order _order;
    std::string _collate;
};

class OrderByClause {
public:
    typedef std::deque<OrderByTerm> List;

    OrderByClause() : _terms (new List()) {}
    ~OrderByClause() {}

    std::string getGenerated();
    void renderTo(QueryTemplate& qt) const;
    boost::shared_ptr<OrderByClause> copySyntax();

private:
    friend std::ostream& operator<<(std::ostream& os, OrderByClause const& oc);
    friend class ModFactory;

    void _addTerm(OrderByTerm const& t) {_terms->push_back(t); }
    boost::shared_ptr<List> _terms;
};

class HavingClause {
public:
    HavingClause() {}
    ~HavingClause() {}

    std::string getGenerated();
    void renderTo(QueryTemplate& qt) const;
    boost::shared_ptr<HavingClause> copySyntax();

private:
    friend std::ostream& operator<<(std::ostream& os, HavingClause const& h);
    friend class ModFactory;
    std::string _expr;
};

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_SELECTLIST_H

