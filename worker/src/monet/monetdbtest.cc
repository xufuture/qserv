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
