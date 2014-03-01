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
#include "cssInterfaceImplDummy.h"
#include "cssInterfaceImplZoo.h"

using std::cout;
using std::endl;
using std::map;
using std::string;
using std::vector;

namespace qCss = lsst::qserv::master;

/**
  * Initialize the Store, the Store will use Zookeeper-based interface, this is
  * for production use.
  *
  * @param connInfo connection information
  */
qCss::Store::Store(string const& connInfo) {
    _cssI = new qCss::CssInterfaceImplZoo(connInfo);
}

/**
  * Initialize the Store, the Store will use Zookeeper-based interface, but will
  * place all data in some non-standard location, use this constructor for testing.
  *
  * @param connInfo connection information
  * @param prefix, for testing, to avoid polluting production setup
  */
qCss::Store::Store(string const& connInfo, string const& prefix) :
    _prefix(prefix) {
    _cssI = new qCss::CssInterfaceImplZoo(connInfo);
}

/**
  * Initialize the Store with dummy interface, use this constructor for testing.
  *
  * @param connInfo connection information
  */
qCss::Store::Store(map<string, string>& kw) {
    _cssI = new qCss::CssInterfaceImplDummy(kw);
}

qCss::Store::~Store() {
    delete _cssI;
}

/** Checks if a given database is registered in the qserv metadata.
  *
  * @param dbName database name
  *
  * @return returns if given database is registered.
  */
bool
qCss::Store::checkIfContainsDb(string const& dbName) {
    string p = _prefix + "/DATABASES/" + dbName;
    bool ret =  _cssI->exists(p);
    cout << "*** checkIfContainsDb(" << dbName << "): " << ret << endl;
    return ret;
}

/** Checks if a given table is registered in the qserv metadata. Throws exception
  * if the database does not exist.
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns if the database contains the table.
  */
bool
qCss::Store::checkIfContainsTable(string const& dbName, string const& tableName) {
    cout << "*** checkIfContainsTable(" << dbName << ", " << tableName << ")"<<endl;
    _validateDbExists(dbName);
    return _checkIfContainsTable(dbName, tableName);
}

/** Checks if a given table is chunked. Throws exception if a given database and/or
  * table does not exist.
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns true if the table is chunked.
  */
bool
qCss::Store::checkIfTableIsChunked(string const& dbName, string const& tableName) {
    cout << "checkIfTableIsChunked " << dbName << " " << tableName << endl;
    try { // FIXME: remove it, it SHOULD throw an exception
        _validateDbTbExists(dbName, tableName);
    } catch (qCss::CssException& e) {
        if (e.errCode()==qCss::CssException::DB_DOES_NOT_EXIST) {
            return false;
        }
    }
    return _checkIfTableIsChunked(dbName, tableName);
}

/** Checks if a given table is subchunked
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns true if the table is subchunked.
  */
bool
qCss::Store::checkIfTableIsSubChunked(string const&dbName, string const&tableName) {
    cout << "checkIfTableIsSubChunked(" << dbName << ", " << tableName << ")"<<endl;
    _validateDbTbExists(dbName, tableName);
    return _checkIfTableIsSubChunked(dbName, tableName);
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
  * @return Returns a vector of table names that are chunked.
  */
vector<string>
qCss::Store::getChunkedTables(string const& dbName) {
    cout << "*** getChunkedTables(" << dbName << ")" << endl;
    _validateDbExists(dbName);
    string p = _prefix + "/DATABASES/" + dbName + "/TABLES";
    vector<string> ret, v = _cssI->getChildren(p);
    vector<string>::const_iterator itr;
    for (itr = v.begin() ; itr != v.end(); ++itr) {
        if (checkIfTableIsChunked(dbName, *itr)) {
            cout << "*** getChunkedTables: " << *itr << endl;
            ret.push_back(*itr);
        }
    }
    return ret;
}

/** Gets subchunked tables
  *
  * @param dbName database name
  *
  * @return Returns a vector of table names that are subchunked.
  */
vector<string>
qCss::Store::getSubChunkedTables(string const& dbName) {
    cout << "*** getSubChunkedTables(" << dbName << ")" << endl;
    _validateDbExists(dbName);
    string p = _prefix + "/DATABASES/" + dbName + "/TABLES";
    vector<string> ret, v = _cssI->getChildren(p);
    vector<string>::const_iterator itr;
    for (itr = v.begin() ; itr != v.end(); ++itr) {
        if (checkIfTableIsSubChunked(dbName, *itr)) {
            cout << "*** getSubChunkedTables: " << *itr << endl;
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
  * @return Returns a 3-element vector with column names: ra, decl, objectId.
  */
vector<string>
qCss::Store::getPartitionCols(string const& dbName, string const& tableName) {
    cout << "*** getPartitionCols(" << dbName << ", " << tableName << ")" << endl;
    _validateDbTbExists(dbName, tableName);
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
    cout << "*** getPartitionCols: " << v[0] << ", " << v[1] << ", " 
         << v[2] << endl;
    return ret;
}

/** Gets chunking level for a particular database.table.
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return Returns 0 if not partitioned, 1 if chunked, 2 if subchunked.
  */
int
qCss::Store::getChunkLevel(string const& dbName, string const& tableName) {
    cout << "getChunkLevel(" << dbName << ", " << tableName << ")" << endl;
    try { // FIXME: remove it, it SHOULD throw an exception
        _validateDbTbExists(dbName, tableName);
    } catch (qCss::CssException& e) {
        if (e.errCode()==qCss::CssException::DB_DOES_NOT_EXIST) {
            return -1;
        }
    }
    bool isChunked = _checkIfTableIsChunked(dbName, tableName);
    bool isSubChunked = _checkIfTableIsSubChunked(dbName, tableName);
    if (isSubChunked) {
        cout << "getChunkLevel returns 2" << endl;
        return 2;
    }
    if (isChunked) {
        cout << "getChunkLevel returns 1" << endl;
        return 1;
    }
        cout << "getChunkLevel returns 0" << endl;
    return 0;
}

/** Retrieve the key column for a database. Throws exception if the database
  * or table does not exist.
  *
  * @param db database name
  * @param table table name
  *
  * @return the name of the partitioning key column
  */
string
qCss::Store::getKeyColumn(string const& dbName, string const& tableName) {
    cout << "*** Store::getKeyColumn(" << dbName << ", " << tableName << ")"<<endl;
    try { // FIXME: remove it, it SHOULD throw an exception
        _validateDbTbExists(dbName, tableName);
    } catch (qCss::CssException& e) {
        if (e.errCode()==qCss::CssException::DB_DOES_NOT_EXIST) {
            return "";
        }
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
    cout << "Store::getKeyColumn, returning: " << ret << endl;
    return ret;
}


/** Validates if database exists. Throw exception if it does not.
 */
void
qCss::Store::_validateDbExists(string const& dbName) {
    if (!checkIfContainsDb(dbName)) {
        cout << "db " << dbName << " not found" << endl;
        throw CssException(CssException::DB_DOES_NOT_EXIST, dbName);
    }
}

/** Validates if table exists. Throw exception if it does not.
    Does not check if the database exists.
 */
void
qCss::Store::_validateTbExists(string const& dbName, string const& tableName) {
    if (!checkIfContainsTable(dbName, tableName)) {
        cout << "table " << dbName << "." << tableName << " not found" << endl;
        throw CssException(CssException::TB_DOES_NOT_EXIST, dbName+"."+tableName);
    }
}

/** Validate if database and table exist. Throw exception if either of them 
    does not.
 */
void
qCss::Store::_validateDbTbExists(string const& dbName, string const& tableName) {
    _validateDbExists(dbName);
    _validateTbExists(dbName, tableName);
}

/** Checks if a given database contains a given table. Does not check if the
    database exists.
*/
bool
qCss::Store::_checkIfContainsTable(string const& dbName, string const& tableName) {
    string p = _prefix + "/DATABASES/" + dbName + "/TABLES/" + tableName;
    bool ret = _cssI->exists(p);
    cout << "*** checkIfContainsTable returns: " << ret << endl;
    return ret;
}

/** Checks if a given table is chunked. Does not check if database and/or table
    exist.
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns true if the table is chunked, false otherwise.
  */
bool
qCss::Store::_checkIfTableIsChunked(string const& dbName, string const& tableName) {
    string p = _prefix + "/DATABASES/" + dbName + "/TABLES/" + 
               tableName + "/partitioning";
    string retS = _cssI->get(p);
    bool retV = (retS != "-1");
    cout << "*** " << dbName << "." << tableName << " is ";
    if (!retV) cout << "NOT ";
    cout << "chunked" << endl;
    return retV;
}

/** Checks if a given table is subchunked. Does not check if database and/or table
    exist.
  *
  * @param dbName database name
  * @param tableName table name
  *
  * @return returns true if the table is subchunked, false otherwise.
  */
bool
qCss::Store::_checkIfTableIsSubChunked(string const& dbName, 
                                       string const& tableName) {
    string p = _prefix + "/DATABASES/" + dbName + "/TABLES/" + 
               tableName + "/partitioning/" + "subChunks";
    string retS = _cssI->get(p);
    bool retV = (retS == "1");
    cout << "*** " << dbName << "." << tableName << " is ";
    if (!retV) cout << "NOT ";
    cout << "subchunked" << endl;
    return retV;
}
