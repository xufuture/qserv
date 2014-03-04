// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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
#ifndef LSST_QSERV_MASTER_TABLEREF_H
#define LSST_QSERV_MASTER_TABLEREF_H
/**
  * @file TableRef.h
  *
  * @brief Declarations for TableRefN and subclasses SimpleTableN and JoinRefN
  *
  * @author Daniel L. Wang, SLAC
  */
#include <stdexcept>
#include <string>
#include <list>
#include <iostream>
#include <boost/shared_ptr.hpp>
#include "query/QueryTemplate.h"

namespace lsst {
namespace qserv {
namespace master {
class QueryTemplate; // Forward
class JoinSpec;
class JoinRef;
typedef std::list<boost::shared_ptr<JoinRef> > JoinRefList;

/// TableRefN is a parsed table reference node
// table_ref :
//   table_ref_aux (options{greedy=true;}:qualified_join | cross_join)*
// table_ref_aux :
//   (n:table_name | /*derived_table*/q:table_subquery) ((as:"as")? c:correlation_name (LEFT_PAREN derived_column_list RIGHT_PAREN)?)?
class TableRef {
public:
    typedef boost::shared_ptr<TableRef> Ptr;
    typedef std::list<Ptr> PtrList;

    TableRef(std::string const& db_, std::string const& table_,
               std::string const& alias_)
        : alias(alias_), db(db_), table(table_)  {
        if(table_.empty()) { throw std::logic_error("TableRef without table"); }
    }
    virtual ~TableRef() {}

    std::ostream& putStream(std::ostream& os) const;
    void putTemplate(QueryTemplate& qt) const;

    bool isSimple() const { return joinRefList.empty(); }
    std::string const& getDb() const { return db; }
    std::string const& getTable() const { return table; }
    std::string const& getAlias() const { return alias; }

    JoinRef const* getJoinRef(int i=0) const;
    JoinRefList const& getJoins() const {
        return joinRefList; }

    // Modifiers
    void setAlias(std::string const& a) { alias=a; }
    void setDb(std::string const& db_) { db = db_; }
    void setTable(std::string const& table_) { table = table_; }
    JoinRefList& getJoins() {
        return joinRefList; }
    void addJoin(boost::shared_ptr<JoinRef> r);

    class Func {
    public:
        virtual ~Func() {}
        virtual void operator()(TableRef& t) {}
    };
    class FuncC {
    public:
        virtual ~FuncC() {}
        virtual void operator()(TableRef const& t) {}
    };
    void applySimple(Func& f);
    void applySimpleRO(FuncC& f) const;

    struct Pfunc {
        virtual std::list<TableRef::Ptr> operator()(TableRef const& t) = 0;
    };
    std::list<TableRef::Ptr> permute(Pfunc& p) const;
    TableRef::Ptr clone() const {
        // FIXME do deep copy.
        return Ptr(new TableRef(*this));
    }

    class render;
private:
    std::string alias;
    std::string db;
    std::string table;
    JoinRefList joinRefList;
};
/// class render: helper functor for QueryTemplate conversion
class TableRef::render {
public:
    render(QueryTemplate& qt) : _qt(qt), _count(0) {}
    void operator()(TableRef const& trn);
    inline void operator()(TableRef::Ptr const trn) {
        if(trn.get()) (*this)(*trn);
    }
    QueryTemplate& _qt;
    int _count;
};

std::ostream& operator<<(std::ostream& os, TableRef const& refN);
std::ostream& operator<<(std::ostream& os, TableRef const* refN);

// Containers
typedef std::list<TableRef::Ptr> TableRefList;
typedef boost::shared_ptr<TableRefList> TableRefListPtr;

}}} // namespace lsst::qserv::master
#endif // LSST_QSERV_MASTER_TABLEREF_H
