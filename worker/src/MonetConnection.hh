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
 
#ifndef LSST_QSERV_SQL_H
#define LSST_QSERV_SQL_H
// sql.h - SQL interface module.  Convenience code/abstraction layer
// fro calling into MySQL.  Uncertain of how this usage conflicts with
// db usage via the python MySQLdb api. 


// Standard
#include <string>
#include <vector>
#include <deque>

// Boost
//#include <boost/thread.hpp>

#include <tr1/memory>

namespace lsst {
namespace qserv {
/// Eventually, make this compatible with SqlErrorObject
struct MonetErrorObj {
    int dummy;
};

class MonetState;// Forward

/// class SqlConfig : Value class for configuring the MySQL connection
class MonetConfig {
public:
    MonetConfig() : port(0) {}
  
    std::string hostname;
    int port;

    std::string username;
    std::string password;
    // std::string lang; // Always "sql"
    std::string db; // ??? 
    bool isValid() const {
        return !username.empty();
    }
};

class MonetResults {
public:
    typedef std::deque<std::string> Strings;
    typedef std::deque<Strings> Strings2;
    MonetResults(bool discardImmediately=false) 
        :_discardImmediately(discardImmediately) {}
    ~MonetResults() {}

#if 0
    void addResult(MYSQL_RES* r);
    bool extractFirstValue(std::string&, SqlErrorObject&);
    bool extractFirstColumn(std::vector<std::string>&, 
                            SqlErrorObject&);
    bool extractFirst2Columns(std::vector<std::string>&, //FIXME: generalize
                              std::vector<std::string>&, 
                              SqlErrorObject&);
    void freeResults();
#endif
    Strings2 const& getResults() const { return _results; }
private:
    void _clear() { _results.clear(); }
    friend class MonetConnection;
    Strings2 _results;
    bool _discardImmediately;

};


/// class MonetConnection : Class for interacting with a MonetDB instanc
class MonetConnection {
public:
    MonetConnection(MonetConfig const& mc);
    ~MonetConnection();
    bool connectToDb(MonetErrorObj&);
    bool selectDb(std::string const& dbName, MonetErrorObj& e);
    bool runQuery(char const* query, int qSize, 
                  MonetResults& results, MonetErrorObj& e);
    // Convenience functions for above.
    bool runQuery(char const* query, int qSize, MonetErrorObj& e);
    bool runQuery(std::string const& query, MonetResults& r, MonetErrorObj& e);
    bool runQuery(std::string const& query, MonetErrorObj& e);
    
    bool dbExists(std::string const& dbName, MonetErrorObj& e);
    bool createDb(std::string const& dbName, MonetErrorObj& e, 
                  bool failIfExists=true);
    bool createDbAndSelect(std::string const& dbName, 
                           MonetErrorObj&, 
                           bool failIfExists=true);
    bool dropDb(std::string const& dbName, MonetErrorObj&,
                bool failIfDoesNotExist=true);
    bool tableExists(std::string const& tableName, 
                     MonetErrorObj&,
                     std::string const& dbName="");
    bool dropTable(std::string const& tableName,
                   MonetErrorObj&,
                   bool failIfDoesNotExist=true,
                   std::string const& dbName="");
    bool listTables(std::vector<std::string>&, 
                    MonetErrorObj&,
                    std::string const& prefixed="",
                    std::string const& dbName="");

    std::string getActiveDbName() const { return _config.db; }

private:
    void _init();
    void _die();
    void _packageResults(MonetResults& r);
    void _flagError();
    bool _setErrorObject(MonetErrorObj&, 
                         std::string const& details=std::string(""));

    std::string _error;
    int _mysqlErrno;
    const char* _mysqlError;
    MonetConfig _config;
    std::tr1::shared_ptr<MonetState> _state;
    Mapi _handle;
    bool _connected;


    static bool _isReady;

}; // class MonetConnection

}} // namespace lsst::qserv
// Local Variables: 
// mode:c++
// comment-column:0 
// End:             

#endif // LSST_QSERV_SQL_H
