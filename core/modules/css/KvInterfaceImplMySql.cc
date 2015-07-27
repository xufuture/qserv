// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
  * @file
  *
  * @brief Interface to the Common State System - MySql-based implementation.
  *
  * @Author Nathan Pease, SLAC
  */


// Class header
#include "css/KvInterfaceImplMysql.h"

// System headers
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>

// Third-party headers
#include "boost/algorithm/string.hpp"
#include "boost/algorithm/string/predicate.hpp"
#include "boost/filesystem.hpp"
#include "boost/format.hpp"
#include "mysql/mysqld_error.h"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "css/CssError.h"
#include "sql/SqlResults.h"
#include "sql/SqlTransaction.h"

using std::map;
using std::string;
using std::vector;

namespace {

// logger instance for this module
LOG_LOGGER _logger = LOG_GET("lsst.qserv.css.KvInterfaceImplMySql");

}


namespace lsst {
namespace qserv {
namespace css {

// todo this is copied more or less directly from QMetaTransaction. Refactor to shared location?
// proper place is to port it into db module
// https://github.com/LSST/db
// right now it's python only.
// create a ticket to make generalized code to be used between c++ and python
class KvTransaction
{
public:
    // Constructors
    KvTransaction(sql::SqlConnection& conn)
        : _errObj(), _trans(conn, _errObj) {
        if (_errObj.isSet()) {
            throw SqlError(_errObj);
        }
    }

    // Destructor
    ~KvTransaction() {
        // instead of just destroying SqlTransaction instance we call abort and see
        // if error happens. We cannot throw here but we can print a message.
        if (_trans.isActive()) {
            _trans.abort(_errObj);
            if (_errObj.isSet()) {
                LOGF(_logger, LOG_LVL_ERROR, "Failed to abort transaction: mysql error: (%1%) %2%" %
                     _errObj.errNo() % _errObj.errMsg());
            }
        }
    }

    /// Explicitly commit transaction, throws SqlError for errors.
    void commit() {
        _trans.commit(_errObj);
        if (_errObj.isSet()) {
            LOGF(_logger, LOG_LVL_ERROR, "Failed to commit transaction: mysql error: (%1%) %2%" %
                 _errObj.errNo() % _errObj.errMsg());
            throw SqlError(_errObj);
        }
    }

    /// Explicitly abort transaction, throws SqlError for errors.
    void abort() {
        _trans.abort(_errObj);
        if (_errObj.isSet()) {
            LOGF(_logger, LOG_LVL_ERROR, "Failed to abort transaction: mysql error: (%1%) %2%" %
                 _errObj.errNo() % _errObj.errMsg());
            throw SqlError(_errObj);
        }
    }

private:
    sql::SqlErrorObject _errObj; // this must be declared before _trans
    sql::SqlTransaction _trans;
};


KvInterfaceImplMySql::KvInterfaceImplMySql(mysql::MySqlConfig const& mysqlConf)
: _conn(mysqlConf) {

}


void
KvInterfaceImplMySql::create(std::string const& key, std::string const& value) {
    KvTransaction transaction(_conn);

    boost::filesystem::path keyPathObj(key);
    boost::filesystem::path parentKeyPathObj = keyPathObj.parent_path();

    std::string parentKey = boost::filesystem::path(key).parent_path().string();

    boost::format fmQuery("INSERT INTO cssData (kvKey, kvVal, parentKey) VALUES ('%1%', '%2%', '%3%');");
    fmQuery % key;
    fmQuery % value;
    fmQuery % parentKey;
    std::string query = fmQuery.str();

    // run query
    sql::SqlErrorObject errObj;
    LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
    if (not _conn.runQuery(query, errObj)) {
        // Header documentation says "Throws CssNoSuchKey if the key is not found."
        // Could test for the error code if we only want to throw KeyExistsError
        // in the case of duplicate entries.
        // TBD what would be thrown for other errors; maybe a generic css::SqlError?
        if (errObj.errNo() == ER_DUP_ENTRY)
        LOGF(_logger, LOG_LVL_ERROR, "SQL INSERT INTO failed: %1%" % query);
        throw KeyExistsError(errObj);
    }

    transaction.commit();
}


void
KvInterfaceImplMySql::set(std::string const& key, std::string const& value) {
    KvTransaction transaction(_conn);

    boost::format fmQuery("INSERT INTO cssData (kvKey, kvVal) VALUES ('%1%', '%2%') ON DUPLICATE KEY UPDATE kvVal='%2%';");
    fmQuery % key;
    fmQuery % value;
    std::string query = fmQuery.str();

    // run query
    sql::SqlErrorObject errObj;
    LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
    if (not _conn.runQuery(query, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "%1% failed with err:%2%" % query % errObj.errMsg());
        throw CssError((boost::format("set error:%1% from query:%2%") % errObj.errMsg() % query).str());
    }

    transaction.commit();
}


bool
KvInterfaceImplMySql::exists(std::string const& key) {
    KvTransaction transaction(_conn);

    std::string val;
    sql::SqlErrorObject errObj;
    return _getValFromServer(key, val, errObj);
}


std::vector<std::string>
KvInterfaceImplMySql::getChildren(std::string const& key) {
    KvTransaction transaction(_conn);

    boost::format fmQuery("SELECT kvKey FROM cssData WHERE parentKey='%1%';");
    fmQuery % key;
    std::string query = fmQuery.str();

    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    LOGF(_logger, LOG_LVL_DEBUG, "getChildren executing query: %1%" % query);
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "getChildren: %1% failed with err:%2%" % query % errObj.errMsg());
        throw CssError((boost::format("getChildren: error:%1% from query:%2%") % errObj.errMsg() % query).str());
    }

    errObj.reset();
    std::vector<std::string> ret;
    if (not results.extractFirstColumn(ret, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "getChildren: failed to extract children from %1% failed with err:%2%" % query % errObj.errMsg());
        throw CssError((boost::format("getChildren: failed to extract children error:%1% from query:%2%") % errObj.errMsg() % query).str());
    }

    transaction.commit();

    return ret;
}


void
KvInterfaceImplMySql::deleteKey(std::string const& key) {
    KvTransaction transaction(_conn);

    sql::SqlErrorObject errObj;
    std::string query = "DELETE FROM cssData WHERE kvKey='";
    query += key;
    query += "';";
    // note: limit the row count to 1 - there should not be multiple rows for any key.

    // run query
    sql::SqlResults resultsObj;
    LOGF(_logger, LOG_LVL_DEBUG, "Executing query: %1%" % query);
    if (not _conn.runQuery(query, resultsObj, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "SQL DELETE FROM failed: %1%" % query);
        throw SqlError(errObj);
    }

    auto affectedRows = resultsObj.getAffectedRows();
    if (affectedRows < 1) {
        LOGF(_logger, LOG_LVL_ERROR, "SQL DELETE FROM failed (no such key): %1%" % query);
        throw NoSuchKey(errObj);
    }
    else if (affectedRows > 1) {
        LOGF(_logger, LOG_LVL_ERROR, "SQL DELETE FROM failed (too many (%1%) rows deleted): %2%" % affectedRows % query);
        throw CssError("deleteKey - unexpectedly deleted more than 1 row.");
    }

    transaction.commit();
}


std::string
KvInterfaceImplMySql::_get(std::string const& key, std::string const& defaultValue, bool throwIfKeyNotFound) {
    std::string val;
    sql::SqlErrorObject errObj;
    if (not _getValFromServer(key, val, errObj)) {
        if (throwIfKeyNotFound) {
            throw NoSuchKey(errObj);
        }
        return defaultValue;
    }
    return val;
}


bool
KvInterfaceImplMySql::_getValFromServer(std::string const& key, std::string & val, sql::SqlErrorObject & errObj)
{
    KvTransaction transaction(_conn);

    bool foundIt(false);

    std::string query = "SELECT kvVal FROM cssData WHERE kvKey='";
    query += key;
    query += "';";

    sql::SqlResults results;
    if (not _conn.runQuery(query, results, errObj)) {
        LOGF(_logger, LOG_LVL_ERROR, "SQL query failed: %1%" % query);
    }
    else {
        errObj.reset();
        std::string value;
        if (results.extractFirstValue(val, errObj)) {
            foundIt = true;
        }
    }

    transaction.commit();
    return foundIt;
}

}}} // namespace lsst::qserv::css
