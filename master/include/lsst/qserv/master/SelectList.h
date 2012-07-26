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
#include <antlr/AST.hpp>
#include <boost/shared_ptr.hpp>

#include "lsst/qserv/master/ColumnRefH.h"
namespace lsst {
namespace qserv {
namespace master {
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
    void printRefs() const {
        std::cout << "Printing select refs." << std::endl;
        typedef std::list<ColumnRef>::const_iterator Citer;
        Citer end = _refs.end();
        for(Citer i=_refs.begin(); i != end; ++i) {
            std::cout << "\t\"" << i->db << "\".\"" 
                      << i->table << "\".\""  << i->column << std::endl;
        }
    }
private:
    std::list<ColumnRef> _refs;
    boost::shared_ptr<ValueExprList> _valueExprList;
};


class FuncExpr {
public:
    std::string getName() const;
    ValueExprList getParams() const;

    std::string name;
    ValueExprList params;
};

typedef boost::shared_ptr<FuncExpr> FuncExprPtr;

class ValueExpr {
public:
    enum Type { COLUMNREF, FUNCTION, AGGFUNC, STAR };

    boost::shared_ptr<ColumnRef> getColumnRef() const { return _columnRef; }
    boost::shared_ptr<FuncExpr> getFuncExpr() const { return _funcExpr; }
    Type getType() const { return _type; }
    
    static ValueExprPtr newColumnRefExpr(boost::shared_ptr<ColumnRef> cr);
    static ValueExprPtr newStarExpr();
    static ValueExprPtr newAggExpr(boost::shared_ptr<FuncExpr> fe);
    static ValueExprPtr newFuncExpr(boost::shared_ptr<FuncExpr> fe);
    friend std::ostream& operator<<(std::ostream& os, ValueExpr const& ve);
    friend std::ostream& operator<<(std::ostream& os, ValueExpr const* ve);

    //private:
    Type _type;
    boost::shared_ptr<ColumnRef> _columnRef;
    boost::shared_ptr<FuncExpr> _funcExpr;
    
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
    void addStar(antlr::RefAST star);
    void addRegular(antlr::RefAST n);
    void addFunc(antlr::RefAST n);
    void addAgg(antlr::RefAST n);
    

    void dbgPrint() const;
    
private:
    void _fillParams(ValueExprList& p, antlr::RefAST pnodes);
    boost::shared_ptr<ColumnRefList> _columnRefList;
    boost::shared_ptr<ValueExprList> _valueExprList;
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
    boost::shared_ptr<ColumnRefList> _columnRefList;
};

class WhereClause {
public:
    WhereClause() : _columnRefList(new ColumnRefList()) {}
    ~WhereClause() {}
    boost::shared_ptr<ColumnRefList> getColumnRefList() {
        return _columnRefList;
    }
    
private:
    boost::shared_ptr<ColumnRefList> _columnRefList;
};

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_SELECTLIST_H

