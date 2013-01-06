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
// See MessageHandler.h
 
#include <iostream>
#include <boost/format.hpp>
#include "lsst/qserv/SqlConnection.hh"
#include "lsst/qserv/master/MessageHandler.h"
using lsst::qserv::SqlErrorObject;
using lsst::qserv::SqlConfig;
using lsst::qserv::SqlConnection;
using lsst::qserv::master::MessageHandler;
using lsst::qserv::master::MessageHandlerError;
using lsst::qserv::master::MessageHandlerConfig;

namespace { // File-scope helpers

boost::shared_ptr<SqlConfig> makeSqlConfig(MessageHandlerConfig const& c) {
    boost::shared_ptr<SqlConfig> sc(new SqlConfig());
    assert(sc.get());
    sc->username = c.user;
    sc->dbName = c.targetDb;
    sc->socket = c.socket;
    return sc;
}

} // anonymous namespace

std::string const MessageHandler::_createSql("CREATE TABLE IF NOT EXISTS %s (code SMALLINT, message CHAR(255)) ENGINE=MEMORY;");
std::string const MessageHandler::_insertSql("INSERT INTO %s VALUES (%d, '%s');");

////////////////////////////////////////////////////////////////////////
// public
////////////////////////////////////////////////////////////////////////
MessageHandler::MessageHandler(MessageHandlerConfig const& c) 
    : _config(c),
      _sqlConfig(makeSqlConfig(c)),
      _messageCount(0) {
    _applySql((boost::format(_createSql) % _config.targetTable).str());
}

void MessageHandler::writeMessage(int code, std::string const& message) {
    boost::lock_guard<boost::mutex> g(_countMutex);
    ++_messageCount;
    _applySql((boost::format(_insertSql) % _config.targetTable % code % message).str());
}

////////////////////////////////////////////////////////////////////////
// private
////////////////////////////////////////////////////////////////////////
bool MessageHandler::_applySql(std::string const& sql) {
    boost::lock_guard<boost::mutex> m(_sqlMutex);
    SqlErrorObject errObj;
    if(!_sqlConn.get()) {
        _sqlConn.reset(new SqlConnection(*_sqlConfig, true));
        if(!_sqlConn->connectToDb(errObj)) {
            _error.status = MessageHandlerError::MYSQLCONNECT;
            _error.errorCode = errObj.errNo();
            _error.description = "Error connecting to db. " + errObj.printErrMsg();
            _sqlConn.reset();
            return false;
        } else {
            std::cout << "MessageHandler " << (void*) this << " connected to db." << std::endl;
        }
    }
    if(!_sqlConn->runQuery(sql, errObj)) {
        _error.status = MessageHandlerError::MYSQLEXEC;
        _error.errorCode = errObj.errNo();
        _error.description = "Error applying sql. " + errObj.printErrMsg();
        return false;
    }
    return true;
}
