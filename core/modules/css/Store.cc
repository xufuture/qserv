// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

/**
  * @file Store.cc
  *
  * @brief A store that manage information from Central State System for Qserv.
  *
  * @Author Jacek Becla, SLAC
  *
  */


// Standard library imports
#include <iostream>

// Local imports
#include "Store.h"
#include "cssException.h"
#include "cssInterfaceImplZoo.h"

using std::cout;
using std::endl;
using std::string;
using std::vector;

namespace qCss = lsst::qserv::master;

/**
  * Initialize the Store.
  *
  * @param connInfo connection information
  */
qCss::Store::Store(string const& connInfo) {
    _cssI = new qCss::CssInterfaceImplZoo(connInfo);
}

/**
  * Initialize the Store.
  *
  * @param connInfo connection information
  * @param prefix, for testing, to avoid polluting production setup
  */
qCss::Store::Store(string const& connInfo, string const& prefix) :
    _prefix(prefix) {
    _cssI = new qCss::CssInterfaceImplZoo(connInfo);
}

qCss::Store::~Store() {
    delete _cssI;
}

/** Checks if a given database is registered in the qserv metadata.
  *
  * @param dbName database name
  *
  * @return returns true or false
  */
bool
qCss::Store::checkIfContainsDb(string const& dbName) {
    string p = _prefix + "/DATABASES/" + dbName;
    cout << "*** checkIfContainsDb() " << p << endl;
    return _cssI->exists(p);
}

/** Checks if a given table is registered in the qserv metadata.
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns true or false
  */
bool
qCss::Store::checkIfContainsTable(string const& dbName, string const& tableName) {
    if (!checkIfContainsDb(dbName)) {
        throw CssException(CssException::DB_DOES_NOT_EXIST, dbName);
    }       
    string p = _prefix + "/DATABASES/" + dbName + "/TABLES/" + tableName;
    return _cssI->exists(p);
}

/** Checks if a given table is chunked
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns true or false
  */
bool
qCss::Store::checkIfTableIsChunked(string const& dbName, string const& tableName) {
    if (!checkIfContainsDb(dbName)) {
        throw CssException(CssException::DB_DOES_NOT_EXIST, dbName);
    }
    if (!checkIfContainsTable(dbName, tableName)) {
        throw CssException(CssException::TB_DOES_NOT_EXIST, dbName+"."+tableName);
    }
    string p = _prefix + "/DATABASES/" + dbName + "/TABLES/" + 
               tableName + "/partitioning";
    return _cssI->exists(p);
}

/** Checks if a given table is subchunked
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns true or false
  */
bool
qCss::Store::checkIfTableIsSubChunked(string const&dbName, string const&tableName) {
    if (!checkIfContainsDb(dbName)) {
        throw CssException(CssException::DB_DOES_NOT_EXIST, dbName);
    }
    if (!checkIfContainsTable(dbName, tableName)) {
        throw CssException(CssException::TB_DOES_NOT_EXIST, dbName+"."+tableName);
    }
    string p = _prefix + "/DATABASES/" + dbName + "/TABLES/" + 
               tableName + "/partitioning/" + "subChunks";
    return _cssI->exists(p);
}

/** Gets allowed databases (database that are configured for qserv)
  *
  * @return returns a vector of database names that are configured for qserv
  */
vector<string>
qCss::Store::getAllowedDbs() {
    string p = _prefix + "/DATABASES";
    return _cssI->getChildren(p);
}

/** Gets chunked tables
  *
  * @param dbName database name
  *
  * @return returns a vector of table names that are chunked
  */
vector<string>
qCss::Store::getChunkedTables(string const& dbName) {
    if (!checkIfContainsDb(dbName)) {
        throw CssException(CssException::DB_DOES_NOT_EXIST, dbName);
    }
    string p = _prefix + "/DATABASES/" + dbName + "/TABLES";
    vector<string> ret, v = _cssI->getChildren(p);

    vector<string>::const_iterator itr;
    for (itr = v.begin() ; itr != v.end(); ++itr) {
        if (checkIfTableIsChunked(dbName, *itr)) {
            ret.push_back(*itr);
        }
    }
    return ret;
}

/** Gets subchunked tables
  *
  * @param dbName database name
  *
  * @return returns a vector of table names that are subchunked
  */
vector<string>
qCss::Store::getSubChunkedTables(string const& dbName) {
    if (!checkIfContainsDb(dbName)) {
        throw CssException(CssException::DB_DOES_NOT_EXIST, dbName);
    }
    string p = _prefix + "/DATABASES/" + dbName + "/TABLES";
    vector<string> ret, v = _cssI->getChildren(p);

    vector<string>::const_iterator itr;
    for (itr = v.begin() ; itr != v.end(); ++itr) {
        if (checkIfTableIsSubChunked(dbName, *itr)) {
            ret.push_back(*itr);
        }
    }
    return ret;
}

/** Gets names of partition columns (ra, decl, objectId) for a given database/table.
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns a 3-element vector with column names: ra, decl, objectId
  */
vector<string>
qCss::Store::getPartitionCols(string const& dbName, string const& tableName) {
    if (!checkIfContainsDb(dbName)) {
        throw CssException(CssException::DB_DOES_NOT_EXIST, dbName);
    }
    if (!checkIfContainsTable(dbName, tableName)) {
        throw CssException(CssException::TB_DOES_NOT_EXIST, dbName+"."+tableName);
    }
    string p = _prefix + "/DATABASES/" + dbName + "/TABLES/" + 
               tableName + "/partitioning/";
    vector<string> v, ret;
    v.push_back("lonColName");
    v.push_back("latColName");
    v.push_back("secIndexColName");
    vector<string>::const_iterator itr;
    for (itr = v.begin() ; itr != v.end(); ++itr) {
        string p1 = p + *itr;
        string s = _cssI->get(p1);
        ret.push_back(s);
    }
    return ret;
}

/** Gets chunking level for a particular database.table.
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns 0 if not partitioned, 1 if chunked, 2 if subchunked.
  */
int
qCss::Store::getChunkLevel(string const& dbName, string const& tableName) {
    if (!checkIfContainsDb(dbName)) {
        throw CssException(CssException::DB_DOES_NOT_EXIST, dbName);
    }
    if (!checkIfContainsTable(dbName, tableName)) {
        throw CssException(CssException::TB_DOES_NOT_EXIST, dbName+"."+tableName);
    }

    return 0; // fixme
}

/** Retrieve the key column for a database
  *
  * @param db database name
  * @param table table name
  *
  * @return the name of the partitioning key column
  */
string
qCss::Store::getKeyColumn(string const& dbName, string const& tableName) {
    if (!checkIfContainsDb(dbName)) {
        throw CssException(CssException::DB_DOES_NOT_EXIST, dbName);
    }
    if (!checkIfContainsTable(dbName, tableName)) {
        throw CssException(CssException::TB_DOES_NOT_EXIST, dbName+"."+tableName);
    }
    string ret, p = _prefix + "/DATABASES/" + dbName + "/TABLES/" + tableName +
                    "/partitioning/secIndexColName";
    try {
        ret = _cssI->get(p);
    } catch (CssException& e) {
        if (e.errCode()==CssException::KEY_DOES_NOT_EXIST) {
            ret = "";
        }
        throw;
    }
    return ret;
}
