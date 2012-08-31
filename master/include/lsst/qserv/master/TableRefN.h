// -*- LSST-C++ -*-
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
// TableRefN is a table ref node in a parsed query.
#ifndef LSST_QSERV_MASTER_TABLEREFN_H
#define LSST_QSERV_MASTER_TABLEREFN_H
#include <string>
#include <list>
#include <boost/shared_ptr.hpp>

namespace lsst {
namespace qserv {
namespace master {

class TableRefN {
public:
    typedef boost::shared_ptr<TableRefN> Ptr;
    virtual ~TableRefN() {}
    virtual std::string const& getAlias() const { return alias; }
    virtual std::string const& getDb() const = 0;
    virtual std::string const& getTable() const = 0;
    virtual std::ostream& putStream(std::ostream& os) const = 0;

protected:
    TableRefN(std::string const& alias_) : alias(alias_) {}
    std::string alias;
    
};
// (implemented in SelectFactory.cc for now)
std::ostream& operator<<(std::ostream& os, TableRefN const& refN);



class SimpleTableN : public TableRefN {
public:
    typedef boost::shared_ptr<SimpleTableN> Ptr;
    SimpleTableN(std::string const& db_, std::string const& table_,
                 std::string const& alias_) 
        : TableRefN(alias_), db(db_), table(table_)  {}
    
    virtual std::string const& getDb() const { return db; }
    virtual std::string const& getTable() const { return table; }
    virtual std::ostream& putStream(std::ostream& os) const {
        os << "Table(" << db << "." << table << ")";
        if(!alias.empty()) { os << " AS " << alias; }
        return os;
    }
protected:
//    std::string alias; // inherited
    std::string db;
    std::string table;
};

class JoinRefN : public TableRefN {
public:
    enum JoinType {DEFAULT, INNER, LEFT, RIGHT, NATURAL, CROSS, FULL};

    JoinRefN(std::string const& db1_, std::string const& table1_, 
             std::string const& db2_, std::string const& table2_,
             JoinType jt, std::string const& condition_, 
             std::string const& alias_) 
        : TableRefN(alias_),
          db1(db1_), table1(table1_), db2(db2_), table2(table2_),
          joinType(jt), condition(condition_) {}

    virtual std::string const& getTable() const { 
        static std::string s;
        return s; 
    }
    virtual std::string const& getDb() const { return getTable(); }

    JoinType getJoinType() const { return joinType; }
    std::string getDb1() { return db1; }
    std::string getDb2() { return db2; }
    std::string getTable1() { return table1; }
    std::string getTable2() { return table2; }
    std::string getCondition() { return condition; }

    virtual std::ostream& putStream(std::ostream& os) const {
        os << "Join(" << db1 << "." << table1 << ", "
           << db2 << "." << table2 << ", " << condition << ")";
        if(!alias.empty()) { os << " AS " << alias; }
        return os;
    }

protected:
    std::string db1;
    std::string table1;
    std::string db2;
    std::string table2;
    JoinType joinType;
    std::string condition; // for now, use a dumb string.

};



// Containers
typedef std::list<TableRefN> TableRefnList;
typedef boost::shared_ptr<TableRefnList> TableRefnListPtr;

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_TABLEREFN_H
