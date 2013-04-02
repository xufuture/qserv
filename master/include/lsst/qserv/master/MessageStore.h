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
 
/// MessageStore.h declares:
/// 
/// struct MessageStoreError
/// class MessageStore 

/// The MessageStore classes are responsible for maintaining messages associated
/// with a query.

#ifndef LSST_QSERV_MASTER_MESSAGE_STORE_H
#define LSST_QSERV_MASTER_MESSAGE_STORE_H

#include <string>
#include <boost/thread.hpp> // for mutex. 
#include <boost/shared_ptr.hpp> // for mutex. 

namespace lsst {
namespace qserv {
namespace master {

/// struct MessageStoreError - value class for MessageStore error code.
struct MessageStoreError {
public:
    enum {NONE} status;
    int errorCode;
    std::string description;
};

struct QueryMessage {
public:
    QueryMessage(int chunkId_,
                 int code_,
                 std::string description_)
        :  chunkId(chunkId_),
           code(code_),
           description(description_)
    {
    }

    int chunkId;
    int code;
    std::string description;
};

class MessageStore {
public:
    void addMessage(int chunkId, int code, std::string const& description);
    QueryMessage getMessage(int idx);
    int messageCount();
    int messageCount(int code);

private:
    MessageStoreError _error;
    boost::mutex _storeMutex;
    std::vector<QueryMessage> _queryMessages;
};

}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_MESSAGE_STORE_H
