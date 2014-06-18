#include <iostream>

#include "qdisp/QueryRequest.h"

#include "log/Logger.h"
#include "qdisp/QueryReceiver.h"

namespace lsst {
namespace qserv {
namespace qdisp {

inline void unprovisionSession(XrdSsiSession* session) {
    if(session) {
        bool ok = session->Unprovision();
        if(!ok) std::cout << "Error unprovisioning\n";
        std::cout << "Unprovision ok.\n";
    }
}

////////////////////////////////////////////////////////////////////////
// QueryRequest::Canceller
////////////////////////////////////////////////////////////////////////
class QueryRequest::Canceller : public util::VoidCallable<void> {
public:
    Canceller(QueryRequest& qr) : _queryRequest(qr) {}
    virtual void operator()() {
        _queryRequest.Finished(true); // Abort using XrdSsiRequest interface
    }
private:
    QueryRequest& _queryRequest;
};
////////////////////////////////////////////////////////////////////////
// QueryRequest
////////////////////////////////////////////////////////////////////////
QueryRequest::QueryRequest(XrdSsiSession* session,
                           std::string const& payload,
                           boost::shared_ptr<QueryReceiver> receiver)
    : _session(session),
      _payload(payload),
      _receiver(receiver) {
    _registerSelfDestruct();
    std::cout << "New QueryRequest with payload(" << payload.size() << ")\n";
}

QueryRequest::~QueryRequest() {
    unprovisionSession(_session);
}

// content of request data
char* QueryRequest::GetRequest(int& requestLength) {
    requestLength = _payload.size();
    std::cout << "Requesting [" << requestLength << "] "
              << _payload << std::endl;
    // Andy promises that his code won't corrupt it.
    return const_cast<char*>(_payload.data());
}

void QueryRequest::RelRequestBuffer() {
    std::cout << "Early release of request buffer\n";
    _payload.clear();
}
// precondition: rInfo.rType != isNone
// Must not throw exceptions: calling thread cannot trap them.
bool QueryRequest::ProcessResponse(XrdSsiRespInfo const& rInfo, bool isOk) {
    std::string errorDesc;
    if(!isOk) {
        _receiver->errorFlush(std::string("Request failed"), -1);
        _errorFinish();
        return true;
    }
    std::cout << "Response type is " << rInfo.State() << std::endl;;
    switch(rInfo.rType) {
    case XrdSsiRespInfo::isNone: // All responses are non-null right now
        errorDesc += "Unexpected XrdSsiRespInfo.rType == isNone";
        break;
    case XrdSsiRespInfo::isData: // Local-only
        errorDesc += "Unexpected XrdSsiRespInfo.rType == isData";
        break;
    case XrdSsiRespInfo::isError: // isOk == true
        //errorDesc += "isOk == true, but XrdSsiRespInfo.rType == isError";
        return _importError(std::string(rInfo.eMsg), rInfo.eNum);
    case XrdSsiRespInfo::isFile: // Local-only
        errorDesc += "Unexpected XrdSsiRespInfo.rType == isFile";
        break;
    case XrdSsiRespInfo::isStream: // All remote requests
        return _importStream();
    default:
        errorDesc += "Out of range XrdSsiRespInfo.rType";
    }
    return _importError(errorDesc, -1);
}

bool QueryRequest::_importStream() {
    _resetBuffer();
    std::cout << "GetResponseData with buffer of " << _bufferRemain << "\n";

    bool retrieveInitiated = GetResponseData(_cursor, _bufferRemain); // Step 6
    LOGGER_INF << "Initiated request "
               << (retrieveInitiated ? "ok" : "err")
               << std::endl;
    if(!retrieveInitiated) {
        bool ok = Finished();
        // delete this; // Don't delete! need to stay alive for error.
        // Not sure when to delete.
        if(ok) {
            _errorDesc += "Couldn't initiate result retr (clean)";
        } else {
            _errorDesc += "Couldn't initiate result retr (UNCLEAN)";
        }
    } else {
        return true;
    }
}

bool QueryRequest::_importError(std::string const& msg, int code) {
    _receiver->errorFlush(msg, code);
    _errorFinish();
    return true;
}

void QueryRequest::ProcessResponseData(char *buff, int blen, bool last) { // Step 7
    std::cout << "ProcessResponse[data] with buflen=" << blen << std::endl;
    if(blen < 0) { // error, check errinfo object.
        int eCode;
        std::cout << " Got an error, eInfo=<" << eInfo.Get(eCode)
             << ">" << std::endl;;
        _receiver->errorFlush("Couldn't retrieve response data", eCode);
        _errorFinish();
        return;
    }
    if(blen > 0) {
        std::cout << std::string(buff, blen) << " [len=" << blen <<"]";
        _cursor = _cursor + blen;
        _bufferRemain = _bufferRemain - blen;
        // Consider flushing when _bufferRemain is small, but non-zero.
        if(_bufferRemain == 0) {
            _receiver->flush(_bufferSize, last);
            _resetBuffer();
        }
        if(!last) {
            bool askAgainOk = GetResponseData(_cursor, _bufferRemain);
            if(!askAgainOk) {
                _errorFinish();
                return;
            }
        }
    }
    if(last || (blen == 0)) {
        std::cout << "all things received, size=" << _bufferSize - _bufferRemain
             << std::endl;
        _receiver->flush(blen, last);
        _finish();
    } else {
        std::cout << "(error) len=" << blen;
    }
    std::cout << (last ? "last=true" : "last=false");
    std::cout << std::endl;;
}
void QueryRequest::_errorFinish() {
    std::cout << "Error finish" << std::endl;;
    bool ok = Finished();
    if(!ok) std::cout << "Error cleaning up QueryRequest\n";
    else std::cout << "Request::Finished() with error (clean).\n";
    //delete this; // ???
}
void QueryRequest::_finish() {
    bool ok = Finished();
    if(!ok) std::cout << "Error with Finished()\n";
    else std::cout << "Finished() ok.\n";
    //delete this; // ???
}

void QueryRequest::_registerSelfDestruct() {
    boost::shared_ptr<Canceller> canceller(new Canceller(*this));
    _receiver->registerCancel(canceller);
}

void QueryRequest::_resetBuffer() {
    _buffer = _receiver->buffer();
    _bufferSize = _receiver->bufferSize();
    _cursor = _buffer;
    _bufferRemain = _bufferSize;
}

std::ostream& operator<<(std::ostream& os, QueryRequest const& r) {
    return os;
}

}}} // lsst::qserv::qdisp
