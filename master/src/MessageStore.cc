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
// See MessageStore.h

#include <iostream>
#include <boost/format.hpp> 
#include "lsst/qserv/master/MessageStore.h"
using lsst::qserv::master::MessageStore;
using lsst::qserv::master::MessageStoreError;
//using lsst::qserv::master::MessageStoreConfig;

namespace qMaster=lsst::qserv::master;

namespace { // File-scope helpers

} // anonymous namespace

////////////////////////////////////////////////////////////////////////
// public
////////////////////////////////////////////////////////////////////////
/*
MessageStore::MessageStore(MessageStoreConfig const& c)
    : _config(c) {

}
*/

void MessageStore::addMessage(int chunkId, int code, std::string const& description) {
    std::cout << "DBG: EXECUTING MessageStore::addMessage(" << chunkId << ", " << code << ", \"" << description << "\")" << std::endl;
    boost::lock_guard<boost::mutex> lock(_storeMutex);
    _queryMessages.insert(_queryMessages.end(), QueryMessage(chunkId, code, description));
    std::cout << "DBG: EXITING  MessageStore::addMessage()" << std::endl;
}

qMaster::QueryMessage MessageStore::getMessage(int idx) {
    std::cout << "DBG: EXECUTING MessageStore::getMessage(" << idx << ")" << std::endl;
    return _queryMessages.at(idx);
}

int MessageStore::messageCount() {
    std::cout << "DBG: EXECUTING MessageStore::messageCount(), returning " << _queryMessages.size() << std::endl;
    return _queryMessages.size();
}

int MessageStore::messageCount(int code) {
    int count = 0;
    for (std::vector<QueryMessage>::const_iterator i = _queryMessages.begin();
         i != _queryMessages.end(); i++) {
        if (i->code == code)
            count++;
    }
    return count;
}

////////////////////////////////////////////////////////////////////////
// private
////////////////////////////////////////////////////////////////////////
