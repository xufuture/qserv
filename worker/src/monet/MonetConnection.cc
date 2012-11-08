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
 
#include "lsst/qserv/worker/MonetConnection.hh"
#include <fstream>
#include <iostream>
#include <sstream>
#include <mapi.h> // MonetDb MAPI (low-level C API)

#include "lsst/qserv/SqlErrorObject.hh"

using lsst::qserv::MonetConfig;
using lsst::qserv::MonetConnection;
using lsst::qserv::MonetErrorObj;
using lsst::qserv::MonetResults;

namespace {
}

////////////////////////////////////////////////////////////////////////
// class MonetState
////////////////////////////////////////////////////////////////////////
// Abstract data class that holds MonetDB-specific state.
class lsst::qserv::MonetState {
public:
    MonetState() {}
    Mapi dbh;
    MapiHdl hdl; // ???
};
////////////////////////////////////////////////////////////////////////
// class MonetConnection
////////////////////////////////////////////////////////////////////////
MonetConnection::MonetConnection(MonetConfig const& c) {
    _config = c;
    _state.reset(new MonetState());
    _state->hdl = NULL;
    _connected = false;
    _init();
}

MonetConnection::~MonetConnection() {
    // Consider trapping exceptions here so that destruction
    // is exception-safe.
    if(_state->hdl != NULL) {
        mapi_close_handle(_state->hdl);
    }  
    if (_state->dbh != NULL) {
        mapi_destroy(_state->dbh);
    } 
}

void MonetConnection::_init() {
    _state->dbh = mapi_connect(_config.hostname.c_str(), 
                               _config.port, 
                               _config.username.c_str(), 
                               _config.password.c_str(),
                               "sql", _config.db.c_str());
    mapi_setAutocommit(_state->dbh, 1);
    if(mapi_error(_state->dbh)) {
        _die();
    }

    _connected = true;
}

void MonetConnection::_die() {
    // This should be something that leverages _flagError();
    if(_state->hdl != NULL) {
        mapi_explain_query(_state->hdl, stderr);
        do {
            if (mapi_result_error(_state->hdl) != NULL)
                mapi_explain_result(_state->hdl, stderr);
        } while (mapi_next_result(_state->hdl) == 1);
        mapi_close_handle(_state->hdl);
        _state->hdl = NULL;
        mapi_destroy(_state->dbh);
    } else if (_state->dbh != NULL) {
        mapi_explain(_state->dbh, stderr);
        mapi_destroy(_state->dbh);
    } else {
        fprintf(stderr, "command failed\n");
    }    
}

void MonetConnection::_flagError(SqlErrorObject& e) {
    // We need to distinguish between errors ruin the db connection
    // and calling client errors that don't force us to re-initialize
    // the connection. 
    if(_state->hdl != NULL) {
        do {
            if (mapi_result_error(_state->hdl) != NULL)
                e.addErrMsg(mapi_result_error(_state->hdl));
        } while (mapi_next_result(_state->hdl) == 1);
        mapi_close_handle(_state->hdl);
        _state->hdl = NULL;
    } 
}

void
MonetConnection::_packageResults(MonetResults& r) {
    MapiHdl& hdl = _state->hdl;
    MonetResults::Strings2& results = r._results;
    int numCols = -1;
    results.clear();
    
    while(mapi_fetch_row(hdl)) {
        if(r._discardImmediately) continue;
        results.push_back(MonetResults::Strings());
        MonetResults::Strings& s = results.back();
        if(numCols == -1) { numCols = mapi_get_field_count(hdl); }
        for(int i=0; i < numCols; ++i) {
            s.push_back(mapi_fetch_field(hdl, i));
        }
    }

}
void MonetConnection::_dumpResults(std::string const& dumpFile) {
    std::ofstream os(dumpFile.c_str());
    MapiHdl& hdl = _state->hdl;
    int numCols = -1;
    std::cerr << "Writing to " << dumpFile << std::endl;
    do {
        while(mapi_fetch_row(hdl)) {
        std::stringstream ss;
        int c=0;
        if(numCols == -1) { numCols = mapi_get_field_count(hdl); }
        for(int i=0; i < numCols; ++i) {
            ss << "'" << mapi_fetch_field(hdl,i) << "'";
            if(++c > 1) ss << ",";
        }
        if(c > 0) os << ss.str();
        }
    }   while(mapi_next_result(hdl) == 1); 
    std::cerr << "Done writing " << dumpFile << std::endl;
    os.close();
}

bool 
MonetConnection::runQuery(char const* query, int qSize, 
                          MonetResults& r, SqlErrorObject& e) {
    bool success = _runHelper(query, qSize, e);
    if(success) {
        _packageResults(r);
    }
}

bool MonetConnection::_runHelper(char const* query, int size, SqlErrorObject& e) {
    _state->hdl = NULL;
    if(query[size-1] == '\0') { // Null-terminated?
        _state->hdl = mapi_query(_state->dbh, query);
    } else {
        std::string q(query, size); // Use std::string's null-terminate
        _state->hdl = mapi_query(_state->dbh, q.c_str());
    }
    if(mapi_error(_state->dbh) != MOK) { _flagError(e); return false; }
    return true;
}

bool 
MonetConnection::runQuery(char const* query, int qSize, SqlErrorObject& e) {
    MonetResults r;
    return runQuery(query, qSize, r, e);
}

bool 
MonetConnection::runQueryDump(char const* query, int qSize, 
                          SqlErrorObject& e, std::string const& dumpFile) {
    bool success = _runHelper(query, qSize, e);
    if(success) {
        _dumpResults(dumpFile);
    } else {
        std::cerr << "Query Fail: " << query << std::endl;
    }
}

bool 
MonetConnection::runQuery(std::string const& query, MonetResults& r, SqlErrorObject& e) {
    _state->hdl = NULL;
    _state->hdl = mapi_query(_state->dbh, query.c_str());
    if(mapi_error(_state->dbh) != MOK) { _flagError(e); return false; }
    _packageResults(r);

    return true;
}

bool 
MonetConnection::runQuery(std::string const& query, SqlErrorObject& e) {
    MonetResults r;
    return runQuery(query.c_str(), r, e);
}

bool MonetConnection::dropDb(std::string const& dbName, SqlErrorObject& e,
                             bool failIfDoesNotExist) {
    // Nop right now.
    return true;
}
bool MonetConnection::connectToDb(SqlErrorObject&) {
    return _connected;
}
bool MonetConnection::tableExists(std::string const& tableName, 
                                  SqlErrorObject&,
                                  std::string const& dbName) {
    return true; // FIXME
    //    PFormat("SELECT name FROM tables WHERE name 'table_I_want_to_drop'");
}

bool MonetConnection::dropTable(std::string const& tableName,
                                SqlErrorObject& e,
                                bool failIfDoesNotExist,
                                std::string const& dbName) {
    return true; // forget about dropping right now.
    // Ignores dbName for now.
    if (!_connected) return false;
    // Just try to drop it, and ignore the error.
    std::string sql = "DROP TABLE " + tableName;
    if (!runQuery(sql, e)) {
        return _setErrorObject(e, "Problem executing: " + sql);
    }
    return true;    
}
 
bool 
MonetConnection::_setErrorObject(SqlErrorObject& errObj, 
                               std::string const& extraMsg) {
    if ( ! extraMsg.empty() ) {
        errObj.addErrMsg(extraMsg);
    }
    return false;
}
