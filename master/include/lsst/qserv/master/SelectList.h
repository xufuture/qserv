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

#include "lsst/qserv/master/ColumnRefH.h"
#include "lsst/qserv/master/TableRefN.h"

namespace lsst {
namespace qserv {
namespace master {
// Forward
class ColumnRefMap;
class ColumnAliasMap;

typedef std::pair<antlr::RefAST, antlr::RefAST> NodeBound;
typedef std::map<antlr::RefAST, NodeBound> NodeMap;

class ValueExpr; // forward
typedef boost::shared_ptr<ValueExpr> ValueExprPtr;
typedef std::list<ValueExprPtr> ValueExprList;

struct ColumnRef {
    ColumnRef(std::string db_, std::string table_, std::string column_) 
        : db(db_), table(table_), column(column_) {}
    
    std::string db;
    std::string table;
    std::string column;
    friend std::ostream& operator<<(std::ostream& os, ColumnRef const& cr);
    friend std::ostream& operator<<(std::ostream& os, ColumnRef const* cr);
};

class ColumnRefList : public ColumnRefH::Listener {
public:
    ColumnRefList() {}
    virtual ~ColumnRefList() {}
    virtual void acceptColumnRef(antlr::RefAST d, antlr::RefAST t, 
                                 antlr::RefAST c);
    void setValueExprList(boost::shared_ptr<ValueExprList> vel) {
        _valueExprList = vel;
    }
    boost::shared_ptr<ColumnRef const> getRef(antlr::RefAST r) {
        if(_refs.find(r) == _refs.end()) {
            std::cout << "couldn't find " << tokenText(r) << " in";
            printRefs();
        }
        assert(_refs.find(r) != _refs.end());
        return _refs[r];
    }
    
    void printRefs() const {
        std::cout << "Printing select refs." << std::endl;
        typedef RefMap::const_iterator Citer;
        Citer end = _refs.end();
        for(Citer i=_refs.begin(); i != end; ++i) {
            ColumnRef const& cr(*(i->second));
            std::cout << "\t\"" << cr.db << "\".\"" 
                      << cr.table << "\".\""  
                      << cr.column << "\"" << std::endl;
        }
    }
private:
    typedef std::map<antlr::RefAST, boost::shared_ptr<ColumnRef> > RefMap;
    RefMap _refs;
    
    boost::shared_ptr<ValueExprList> _valueExprList;
};


class FuncExpr {
public:
    std::string getName() const;
    ValueExprList getParams() const;

    std::string name;
    ValueExprList params;
    friend std::ostream& operator<<(std::ostream& os, FuncExpr const& fe);
    friend std::ostream& operator<<(std::ostream& os, FuncExpr const* fe);
};

typedef boost::shared_ptr<FuncExpr> FuncExprPtr;

class ValueExpr {
public:
    enum Type { COLUMNREF, FUNCTION, AGGFUNC, STAR };

    boost::shared_ptr<ColumnRef const> getColumnRef() const { return _columnRef; }
    boost::shared_ptr<FuncExpr> getFuncExpr() const { return _funcExpr; }
    Type getType() const { return _type; }
    
    static ValueExprPtr newColumnRefExpr(boost::shared_ptr<ColumnRef const> cr);
    static ValueExprPtr newStarExpr(std::string const& table);
    static ValueExprPtr newAggExpr(boost::shared_ptr<FuncExpr> fe);
    static ValueExprPtr newFuncExpr(boost::shared_ptr<FuncExpr> fe);
    friend std::ostream& operator<<(std::ostream& os, ValueExpr const& ve);
    friend std::ostream& operator<<(std::ostream& os, ValueExpr const* ve);

    //private:
    Type _type;
    boost::shared_ptr<ColumnRef const> _columnRef;
    boost::shared_ptr<FuncExpr> _funcExpr;
    std::string _alias;
    std::string _tableStar;
};

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
    friend class SelectListFactory;
private:
    void _fillParams(ValueExprList& p, antlr::RefAST pnodes);
    boost::shared_ptr<ColumnRefList> _columnRefList;
    boost::shared_ptr<ValueExprList> _valueExprList;
    boost::shared_ptr<ColumnRefMap const> _aliasMap;
};


struct FromEntry {
    antlr::RefAST alias;
    NodeBound target;
    
};

class FromList {
public:
    FromList() : _columnRefList(new ColumnRefList()) {}
    ~FromList() {}
    boost::shared_ptr<ColumnRefList> getColumnRefList() {
        return _columnRefList;
    }

private:
    friend std::ostream& operator<<(std::ostream& os, FromList const& fl);
    friend class FromFactory;

    boost::shared_ptr<ColumnRefList> _columnRefList;
    TableRefnListPtr _tableRefns;
};

class WhereClause {
public:
    WhereClause() : _columnRefList(new ColumnRefList()) {}
    ~WhereClause() {}
    boost::shared_ptr<ColumnRefList> getColumnRefList() {
        return _columnRefList;
    }

private:
    friend std::ostream& operator<<(std::ostream& os, WhereClause const& wc);
    friend class WhereFactory;

    std::string _original;
    boost::shared_ptr<ColumnRefList> _columnRefList;
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

private:
    friend std::ostream& operator<<(std::ostream& os, OrderByClause const& oc);
    friend class ModFactory;

    void _addTerm(OrderByTerm const& t) {_terms->push_back(t); }
    boost::shared_ptr<List> _terms;
};

class GroupByTerm {
public:
    GroupByTerm() {}
    ~GroupByTerm() {}

    boost::shared_ptr<const ValueExpr> getExpr();
    std::string getCollate() const;

private:
    friend std::ostream& operator<<(std::ostream& os, GroupByTerm const& gb);
    friend class ModFactory;
    
    boost::shared_ptr<ValueExpr> _expr;
    std::string _collate;
};

class GroupByClause {
public:
    typedef std::deque<GroupByTerm> List;

    GroupByClause() : _terms(new List()) {}
    ~GroupByClause() {}

private:
    friend std::ostream& operator<<(std::ostream& os, GroupByClause const& gc);
    friend class ModFactory;

    void _addTerm(GroupByTerm const& t) { _terms->push_back(t); }
    boost::shared_ptr<List> _terms;
};

class HavingClause {
public:
    HavingClause() {}
    ~HavingClause() {}

private:
    friend std::ostream& operator<<(std::ostream& os, HavingClause const& h);
    friend class ModFactory;
    std::string _expr;
};

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_SELECTLIST_H

