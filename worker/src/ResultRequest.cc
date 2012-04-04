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
#include "lsst/qserv/worker/ResultRequest.h"
#include "lsst/qserv/worker/ResultTracker.h"
#include "lsst/qserv/worker/Base.h"
#include "lsst/qserv/worker/QueryRunner.h"
#include "lsst/qserv/QservPath.hh"
#include "XrdSfs/XrdSfsCallBack.hh"

#include <fcntl.h>
#include <arpa/inet.h> // htonl

namespace qWorker = lsst::qserv::worker;

////////////////////////////////////////////////////////////////////////
// anonymous helpers
////////////////////////////////////////////////////////////////////////
namespace {
#if 0
void lookupResult(std::string const& hash) {
    ResultErrorPtr p = QueryRunner::getTracker().getNews(hash);
}
#endif
qWorker::ResultRequest::ReadSize getRealSize(std::string const& filename) {
    struct stat statbuf;
    if (::stat(filename.c_str(), &statbuf) == -1) {
        statbuf.st_size = -errno;
    }
    return statbuf.st_size;
}

} // anonymous namespace



////////////////////////////////////////////////////////////////////////
// XrdFinishListener
////////////////////////////////////////////////////////////////////////
class XrdFinishListener { 
public:
    XrdFinishListener(XrdOucErrInfo* eInfo) {
        _callback = XrdSfsCallBack::Create(eInfo);
    }
    virtual void operator()(qWorker::ResultError const& p) {
        if(p.code == 0) {
            // std::cerr << "Callback=OK!\t" << (void*)_callback << std::endl;
            _callback->Reply_OK();
        } else {
            //std::cerr << "Callback error! " << p.first 
            //	      << " desc=" << p.second << std::endl;
            _callback->Reply_Error(p.code, p.desc.c_str());
        }
        _callback = 0;
        // _callback will be auto-destructed after any Reply_* call.
    }
private:
    XrdSfsCallBack* _callback;
};
////////////////////////////////////////////////////////////////////////
// NullListener
////////////////////////////////////////////////////////////////////////
class NullListener { 
public:
    explicit NullListener(const char* name) : _name(name) {
    }
    virtual void operator()(qWorker::ResultError const& p) {
        if(p.code == 0) {
            std::cerr << "Callback=OK!\t" << _name << std::endl;
        } else {
            std::cerr << "Callback error! " << _name << " " 
                      << p.code << " desc=" << p.desc << std::endl;
        }
    }
private:
    const char* _name;
};
////////////////////////////////////////////////////////////////////////
// class ResultRequest::Frame
////////////////////////////////////////////////////////////////////////

qWorker::ResultRequest::Frame::Frame(std::string const& hash, 
                                       qWorker::ResultRequest::ReadSize dSize,
                                       int chunkId) {
    setup(hash, dSize, chunkId);
}

void
qWorker::ResultRequest::Frame::setup(std::string const& hash, 
                                           qWorker::ResultRequest::ReadSize dSize,
                                           int chunkId) {
    // frameSize = sizeof(headerSize) + sizeof(serialized header)
    // Prepare header
    header.reset(new lsst::qserv::ResultHeader());
    assert(header.get());

    // Fill-in
    header->set_session(0); // FIXME
    lsst::qserv::ResultHeader::Result* res = header->add_result();
    res->add_hash(hash);
    res->set_resultsize(dSize);
    res->add_chunkid(chunkId);
    std::stringstream ss;
    header->SerializeToOstream(&ss);
    ReadSize headerSize = ss.tellp();
    // Compute frame size
    size = headerSize + sizeof(uint32_t);
    
    // Now writeout
    bytes.reset(new char[size]);
    *reinterpret_cast<uint32_t*>(bytes.get()) = htonl(headerSize);

    ss.seekp(0);
    ss.read(bytes.get() + sizeof(uint32_t), headerSize);
    assert(!(ss.fail() || ss.eof()));
    assert(ss.tellp() == headerSize);
}

int qWorker::ResultRequest::Frame::copyTo(int offset, char* buffer, int count) {
    assert(bytes.get());
    int copySize = count;
    if(copySize > size) { copySize = size; }
    memcpy(buffer, bytes.get(), copySize);
    return copySize;
}

////////////////////////////////////////////////////////////////////////
// class ResultRequest
////////////////////////////////////////////////////////////////////////
qWorker::ResultRequest::ResultRequest(QservPath const& p, XrdOucErrInfo* e) 
    :_state(UNKNOWN), _fsFileEinfo(e), _hasRealSize(false), 
     _isHeaderReady(false), _hash(p.hashName()) {
    assert(p.requestType() == QservPath::RESULT);
    _accept(p);
}

/// discards results: clears news in result tracker and deletes dump file
/// @return false if unsuccessful
bool qWorker::ResultRequest::discard() {
    _state = DISCARDED;
    QueryRunner::getTracker().clearNews(_hash);
    // Must remove dump file while we are doing the single-query workaround
    // _eDest->Say((Pformat("Not Removing dump file(%1%)")
    // 		 % _dumpName ).str().c_str());
    int result = ::unlink(_dumpName.c_str());
    if(result != 0) { return false; }
    return true;
}

qWorker::ResultRequest::ResultInfo qWorker::ResultRequest::readWithHeader(ReadSize offset, 
                                                                          char* buffer, 
                                                                          ReadSize bufferSize) {
    // FIXME: In order to provide richer result handling:
    // compute a header, serialize it, then compute all offsets relative to that header.
    // Consider writing out the header's bytes (or saving it in
    // memory--it should be small (< few KB).
    
    // Goals for rich header: allow batching of results.
    // Format: headerSize (4-byte, network byte order), header, result blobs.
    // (Result blob sizes are located in the header)
    // (also, consider that we can do pre-merging on the worker, and
    // return a single dump for multiple dump files, but that is a
    // later optimization) 

    // First pass (to close ticket and move on) should implement
    // checking for rich header in path, and building header and
    // prepending as appropriate.
    if(!_isHeaderReady) {
        if(!_hasRealSize) {
            _realSize = getRealSize(_dumpName);
            _hasRealSize = true;
        }
        int chunkId = 0; // FIXME: Need to get chunkId from resultError
        _frame.setup(_hash, _realSize, chunkId);
    }
    // Step 1: Construct result header (only once during resultrequest
    // lifetime.)
    // 1a: Stat the dump file to get the size
    ReadSize dumpSize;
    // 1b: Construct the result header
    // Step 2: Choose the bytes we need.
    ReadSize framePart = 0;
    if(offset < _frame.size) {
        framePart = _frame.copyTo(offset, buffer, bufferSize);
    }
    ReadSize resultOffset = offset - _frame.size;
    ReadSize spaceLeft = bufferSize - framePart;
    ResultInfo ri = read(resultOffset, buffer + framePart, spaceLeft);
    return ri;
    

}


qWorker::ResultRequest::ResultInfo qWorker::ResultRequest::read(ReadSize offset, 
                                                                char* buffer, 
                                                                ReadSize bufferSize) {
    ResultInfo ri;
    std::string msg;
    std::string fn = _dumpName;
    if(!_hasRealSize) {
        _realSize = getRealSize(fn);
        _hasRealSize = true;
    }
    ri.realSize = _realSize;
    
    int fd = ::open(fn.c_str(), O_RDONLY);
    if (fd == -1) {
        ri.errNo = errno;
        ri.error = this->str() + " [Can't open dumpfile]";
    }  else {
        off_t pos = ::lseek(fd, offset, SEEK_SET);
        if (pos == static_cast<off_t>(-1) || pos != offset) {
            ri.errNo = errno;
            ri.error = "Unable to seek in query results";
        } else {
            ssize_t bytes = ::read(fd, buffer, bufferSize);
            if (bytes == -1) {
                ri.errNo = errno;
                ri.error = "Unable to read query results";
            } else {
                ri.size = bytes;
                ri.errNo = 0;
            } // read()
        } // lseek()
    } // open()
    ::close(fd); // Always close.
    return ri;
}

std::string qWorker::ResultRequest::getStateStr() const {
    std::stringstream ss;
    switch(_state) {
    case UNKNOWN:
        return "Unknown";
    case OPENWAIT:
        return "Waiting for result";
    case OPEN:
        return "Ready";
    case OPENERROR:
        ss << "Error:" << _error;
        return ss.str();
    case DISCARDED:
        return "Discarded";
    default:
        return "Corrupt";
    }
}

std::string
qWorker::ResultRequest::str() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

////////////////////////////////////////////////////////////////////////
// class ResultRequest (private)
////////////////////////////////////////////////////////////////////////
qWorker::ResultRequest::State 
qWorker::ResultRequest::_accept(QservPath const& p) {
    _dumpName = hashToResultPath(p.hashName()); 
    ResultErrorPtr rp = QueryRunner::getTracker().getNews(_hash);
    if(rp.get()) {
        if(rp->code != 0) { // Error, so report it.
            _error = rp->desc;
            _state = OPENERROR;
        } else { _state = OPEN; }
    } else { // No news, so listen for it.
        if(_fsFileEinfo) {
            qWorker::QueryRunner::getTracker().listenOnce(
                p.hashName(), XrdFinishListener(_fsFileEinfo));
        } else {
            qWorker::QueryRunner::getTracker().listenOnce(
                p.hashName(), NullListener("rrGeneric"));                
        }
        _state = OPENWAIT; 
    } 
    return _state;
}

////////////////////////////////////////////////////////////////////////
// class ResultRequest public helper
////////////////////////////////////////////////////////////////////////
std::ostream& 
qWorker::operator<<(std::ostream& os, qWorker::ResultRequest const& rr) {
    os << "ResultRequest " << rr._dumpName << ": " << rr.getStateStr();
    return os;
}
