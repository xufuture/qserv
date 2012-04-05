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
 
#ifndef LSST_QSERV_WORKER_RESULT_REQUEST_H
#define LSST_QSERV_WORKER_RESULT_REQUEST_H
//#include <deque>
#include <iostream>
#include <string>

//#include <boost/signal.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>

#include "lsst/qserv/worker.pb.h"

class XrdOucErrInfo; // forward

namespace lsst {
namespace qserv {
class QservPath; // Forward
namespace worker {

class ResultRequest {
public:
    enum State {UNKNOWN, OPENWAIT, OPEN, OPENERROR, DISCARDED};
    typedef boost::shared_ptr<ResultRequest> Ptr;
    typedef int64_t ReadSize;

    struct ResultInfo { // outcome of read operation
        ReadSize realSize;
        ReadSize size;
        std::string msg;
        std::string error;
        int errNo; // lowercase errno is a macro.
    };

    struct Frame { // Result frame
        Frame() {}
        Frame(std::string const& hash, ReadSize dSize, int chunkId);
        void setup(std::string const& hash, ReadSize dSize, int chunkId);
        int copyTo(int offset, char* buffer, int count);
        
        // Fields
        boost::shared_ptr<ResultHeader> header;
        int size;
        boost::shared_array<char> bytes;   // frame = headerlen + header
    };

    explicit ResultRequest(QservPath const& p, XrdOucErrInfo* e);

    // Modifiers
    bool discard();
    ResultInfo read(ReadSize offset, char* buffer, 
                    ReadSize bufferSize);
    ResultInfo readDumpOnly(ReadSize offset, char* buffer, 
                            ReadSize bufferSize);
    ResultInfo readWithHeader(ReadSize offset, char* buffer, 
                              ReadSize bufferSize);

    // Retrievers
    State getState() const { return _state; }
    int getChunkId() const { return _chunkId; }
    std::string getDumpName() const { return _dumpName; }
    std::string getStateStr() const;
    std::string str() const;
private:
    State _accept(QservPath const& p);
    void _importModifiers(QservPath const& p);

    State _state;
    bool _hasRealSize;
    bool _isHeaderReady;
    ReadSize _realSize; // on-disk dump size
    std::string _hash;
    std::string _dumpName;
    int _chunkId;
    Frame _frame;
    std::string _error;
    // Settings from path modifiers
    bool _useBatch;


    friend std::ostream& 
        operator<<(std::ostream& os, 
                   lsst::qserv::worker::ResultRequest const& rr); 
    XrdOucErrInfo* _fsFileEinfo;
};

    
}}} // namespace lsst::qserv::worker

#endif // LSST_QSERV_WORKER_RESULT_REQUEST_H
