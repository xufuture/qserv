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
 
/// MessageHandler.h declares:
/// 
/// struct MessageHandlerError 
/// class MessageHandlerConfig 
/// class MessageHandler 

/// The MessageHandler classes are responsible for inserting messages associated 
/// with a query result into a mysql database.

#ifndef LSST_QSERV_MASTER_MESSAGE_HANDLER_H
#define LSST_QSERV_MASTER_MESSAGE_HANDLER_H

#include <string>
#include <boost/thread.hpp> // for mutex. 
#include <boost/shared_ptr.hpp> // for mutex. 

namespace lsst {
namespace qserv {
// Forward
class SqlConfig;
class SqlConnection;
}}

namespace lsst {
namespace qserv {
namespace master {

/// struct MessageHandlerError - value class for MessageHandler error code.
struct MessageHandlerError {
public:
    enum {NONE, IMPORT, MYSQLOPEN, MERGEWRITE, TERMINATE, 
          MYSQLCONNECT, MYSQLEXEC} status;
    int errorCode;
    std::string description;
};

/// class MessageHandlerConfig - value class for configuring a MessageHandler
class MessageHandlerConfig {
public:
    MessageHandlerConfig(std::string targetDb_, 
                         std::string targetTable_,
                         std::string user_, 
                         std::string socket_) 
        :  targetDb(targetDb_),  
           targetTable(targetTable_),
           user(user_),  
           socket(socket_)
    {
    }

    std::string targetDb; 
    std::string targetTable;
    std::string user;
    std::string socket;
};

class MessageHandler {
public:
    explicit MessageHandler(MessageHandlerConfig const& c);

    void writeMessage(int code, std::string const& message);

    MessageHandlerError const& getError() const { return _error; }
    std::string getTargetTable() const {return _config.targetTable; }

private:
    bool _applySql(std::string const& sql);

    static std::string const _createSql;
    static std::string const _insertSql;

    MessageHandlerConfig _config;
    boost::shared_ptr<SqlConfig> _sqlConfig;
    boost::shared_ptr<SqlConnection> _sqlConn;

    MessageHandlerError _error;
    int _messageCount;
    boost::mutex _countMutex;
    boost::mutex _sqlMutex;
};

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_MESSAGE_HANDLER_H
