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
 
#include "MonetConnection.hh"
#include <mapi.h> // MonetDb MAPI (low-level C API)

using lsst::qserv::MonetConfig;
using lsst::qserv::MonetConnection;
using lsst::qserv::MonetErrorObj;
using lsst::qserv::MonetResults;

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
    if(mapi_error(_state->dbh)) {
        _die();
    }
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
        mapi_destroy(_state->dbh);
    } else if (_state->dbh != NULL) {
        mapi_explain(_state->dbh, stderr);
        mapi_destroy(_state->dbh);
    } else {
        fprintf(stderr, "command failed\n");
    }    
}

void MonetConnection::_flagError() {
    // We need to distinguish between errors ruin the db connection
    // and calling client errors that don't force us to re-initialize
    // the connection. 
    return; // FIXME
    if(_state->hdl != NULL) {
        mapi_explain_query(_state->hdl, stderr);
        do {
            if (mapi_result_error(_state->hdl) != NULL)
                mapi_explain_result(_state->hdl, stderr);
        } while (mapi_next_result(_state->hdl) == 1);
        mapi_close_handle(_state->hdl);
        mapi_destroy(_state->dbh);
    } else if (_state->dbh != NULL) {
        mapi_explain(_state->dbh, stderr);
        mapi_destroy(_state->dbh);
    } else {
        fprintf(stderr, "command failed\n");
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

bool 
MonetConnection::runQuery(char const* query, int qSize, 
                          MonetResults& r, MonetErrorObj& e) {
  _state->hdl = NULL;
  if(query[qSize-1] == '\0') { // Null-terminated?
      _state->hdl = mapi_query(_state->dbh, query);
  } else {
      std::string q(query, qSize); // Use std::string's null-terminate
      _state->hdl = mapi_query(_state->dbh, q.c_str());
  }
  if(mapi_error(_state->dbh) != MOK) { _flagError(); return false; }
  _packageResults(r);
  return true;
}

bool 
MonetConnection::runQuery(char const* query, int qSize, MonetErrorObj& e) {
    MonetResults r;
    return runQuery(query, qSize, r, e);
}

bool 
MonetConnection::runQuery(std::string const& query, MonetResults& r, MonetErrorObj& e) {
  _state->hdl = NULL;
  _state->hdl = mapi_query(_state->dbh, query.c_str());
  if(mapi_error(_state->dbh) != MOK) { _flagError(); return false; }
  _packageResults(r);
  return true;
}

bool 
MonetConnection::runQuery(std::string const& query, MonetErrorObj& e) {
    MonetResults r;
    return runQuery(query.c_str(), r, e);
}

