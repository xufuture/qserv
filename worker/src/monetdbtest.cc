// Based on sample code taken from:
// http://www.monetdb.org/Documentation/Manuals/SQLreference/Programming/MAPI
// on November 6, 2012.
#include <mapi.h>
#include <stdio.h>
#include <stdlib.h>


#include "MonetConnection.hh"
#include <iostream>

using lsst::qserv::MonetConfig;
using lsst::qserv::MonetConnection;
using lsst::qserv::MonetErrorObj;
using lsst::qserv::MonetResults;

class lsst::qserv::MonetState {
public:
    MonetState() {}
    Mapi dbh;
    MapiHdl hdl; // ???
};

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

namespace { // Tutorial code
void die(Mapi dbh, MapiHdl hdl) {
  if (hdl != NULL) {
    mapi_explain_query(hdl, stderr);
    do {
      if (mapi_result_error(hdl) != NULL)
	mapi_explain_result(hdl, stderr);
    } while (mapi_next_result(hdl) == 1);
    mapi_close_handle(hdl);
    mapi_destroy(dbh);
  } else if (dbh != NULL) {
    mapi_explain(dbh, stderr);
    mapi_destroy(dbh);
  } else {
    fprintf(stderr, "command failed\n");
  }
  exit(-1);
}
     
MapiHdl query(Mapi dbh, char const *q)
{
  MapiHdl ret = NULL;
  if ((ret = mapi_query(dbh, q)) == NULL || mapi_error(dbh) != MOK)
    die(dbh, ret);
  return(ret);
}
     
void update(Mapi dbh, char const *q)
{
  MapiHdl ret = query(dbh, q);
  if (mapi_close_handle(ret) != MOK)
    die(dbh, ret);
}

int doTutorial() { // Tutorial's 'main()' body.
  Mapi dbh;
  MapiHdl hdl = NULL;
  char *name;
  char *age;
     
  dbh = mapi_connect("localhost", 50000, "monetdb", "monetdb", "sql", "demo");
  if (mapi_error(dbh))
    die(dbh, hdl);
     
  update(dbh, "CREATE TABLE emp (name VARCHAR(20), age INT)");
  update(dbh, "INSERT INTO emp VALUES ('John', 23)");
  update(dbh, "INSERT INTO emp VALUES ('Mary', 22)");
     
  hdl = query(dbh, "SELECT * FROM emp");
     
  while (mapi_fetch_row(hdl)) {
    name = mapi_fetch_field(hdl, 0);
    age = mapi_fetch_field(hdl, 1);
    printf("%s is %s\n", name, age);
  }
     
  mapi_close_handle(hdl);
  mapi_destroy(dbh);
}

int tryMonetConnection() { 
    // Try something similar to tutorial using C++ wrapper
    MonetConfig mc;
    mc.hostname = "localhost";
    mc.username = "monetdb";
    mc.password = "monetdb";
    mc.port = 50000;
    mc.db = "voc";
    MonetConnection conn(mc);
    // FIXME: check for validity
    MonetErrorObj e;
    MonetResults r;
    char const createStmt[] = "CREATE TABLE emp (name VARCHAR(20), age INT)";
    if(!conn.runQuery(createStmt, e)) {
        conn.runQuery("DROP TABLE emp;", e);
        conn.runQuery(createStmt, e);
    }
    conn.runQuery("INSERT INTO emp VALUES ('John', 23)", e);
    conn.runQuery("INSERT INTO emp VALUES ('Mary', 22)", e);
    if(conn.runQuery("SELECT * FROM emp", r, e))
    
    typedef MonetResults::Strings Strings;
    typedef MonetResults::Strings2 Strings2;
    Strings2 const& rs = r.getResults();
    for(Strings2::const_iterator i = rs.begin(); i != rs.end(); ++i) {
        std::cout << (*i)[0] << " is " << (*i)[1] << std::endl;
    }
    return 0;
}

} // anonymous namespace     

int main(int argc, char *argv[]) {
    return tryMonetConnection();
    return doTutorial();
}
