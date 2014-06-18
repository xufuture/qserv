// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

/**
  * @file
  *
  * @brief Executive. It executes things.
  *
  * @author Daniel L. Wang, SLAC
  */

#include "qdisp/Executive.h"

// System headers
#include <iostream>
#include <sstream>

// Third-party headers
#include "XrdSsi/XrdSsiErrInfo.hh"
#include "XrdSsi/XrdSsiService.hh" // Resource

// #include "boost/date_time/posix_time/posix_time_types.hpp"
// #include <boost/format.hpp>
// #include <boost/make_shared.hpp>

// // Local headers
// #include "ccontrol/ConfigMap.h"
// #include "css/Facade.h"
//#include "global/stringTypes.h"
#include "global/ResourceUnit.h"
#include "log/Logger.h"
// #include "log/msgCode.h"
// #include "rproc/TableMerger.h"
// #include "qdisp/ChunkQuery.h"
// #include "qdisp/MessageStore.h"
#include "qdisp/MergeAdapter.h"
#include "qdisp/QueryResource.h"
// #include "qproc/QuerySession.h"
// #include "util/Timer.h"
// #include "util/WorkQueue.h"
// #include "xrdc/PacketIter.h"
namespace {
std::string figureOutError(XrdSsiErrInfo & e) {
    std::ostringstream os;
    int errCode;
    os << "XrdSsiError " << e.Get(errCode);
    os <<  " Code=" << errCode;
    return os.str();
}
} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qdisp {

std::ostream& operator<<(std::ostream& os, QueryReceiver const& qr) {
    return qr.print(os);
}

template <typename Ptr>
struct printMapSecond {
    printMapSecond(std::ostream& os_, std::string sep_)
        : os(os_), sep(sep_), first(true)  {}

    void operator()(Ptr const& p) {
        if(!first) { os << sep; }
        os << *(p.second);
        first = true;
    }
    std::ostream& os;
    std::string sep;
    bool first;
};

void Executive::_setup() {
    XrdSsiErrInfo eInfo;
    _service = XrdSsiGetClientService(eInfo, _config.serviceUrl.c_str()); // Step 1
    if(!_service) figureOutError(eInfo);
    assert(_service);
    _requestCount = 0;
}

void Executive::add(int refNum, TransactionSpec const& t,
                    std::string const& resultName) {
    LOGGER_DBG << "EXECUTING Executive::add(TransactionSpec, "
               << resultName << ")" << std::endl;
    Spec s;
    s.resource = ResourceUnit(t.path);
    if(s.resource.unitType() == ResourceUnit::CQUERY) {
        s.resource.setAsDbChunk(s.resource.db(), s.resource.chunk());
    }
    s.request = t.query;
    // FIXME
    s.receiver = MergeAdapter::newInstance(); //savePath, resultName);
    add(refNum, s);
}

void Executive::abort() {
    std::cout << "Trying to cancel all queries...\n";
    std::vector<int> pending;
    {
        boost::lock_guard<boost::mutex> lock(_receiversMutex);
        std::cout << "Loop cancel all queries...\n";
        ReceiverMap::iterator i,e;
        for(i=_receivers.begin(), e=_receivers.end(); i != e; ++i) {
            i->second->cancel();
            pending.push_back(i->first);
        }
        std::cout << "Loop cancel all queries...done\n";
    }
    { // Should be possible to convert this to a for_each call.
        std::vector<int>::iterator i,e;
        for(i=pending.begin(), e=pending.end(); i != e; ++i) {
            _unTrack(*i);
        }
    }
}

void Executive::add(int refNum, Executive::Spec const& s) {
    bool trackOk =_track(refNum, s.receiver); // Remember so we can join.
    if(!trackOk) {
        std::cout << "Ignoring duplicate add(" << refNum << ")\n";
        return;
    }
    QueryResource* r = new QueryResource(s.resource.path(),
                                         s.request,
                                         s.receiver);
    std::cout << "Adding/provisioning " << s.resource.path() << std::endl;
    bool provisionOk = _service->Provision(r);  // 2
    if(!provisionOk) {
        std::cout << "There was an error provisioning " << __FILE__ << __LINE__ << std::endl;;
        figureOutError(r->eInfo);
        _unTrack(refNum);
        delete r;

        // handle error
    }
    std::cout << "Provision was ok\n";
#if 0
    {
        boost::lock_guard<boost::mutex> lock(_queriesMutex);
        _queries[id] = qs;
        ++_queryCount;
    }
    std::string msg = std::string("Query Added: url=") + ts.path + ", savePath=" + ts.savePath;
    getMessageStore()->addMessage(id, log::MSG_MGR_ADD, msg);
    LOGGER_INF << "Added query id=" << id << " url=" << ts.path
               << " with save " << ts.savePath << "\n";
    qs.first->run();
    return id;
#endif
}

bool Executive::join() {
    // To join, we make sure that all of the chunks added so far are complete.
    // Check to see if _receivers is empty, if not, then sleep on a condition.
    _waitUntilEmpty();
    // Okay to merge. probably not the Executive's responsibility
    // _merger->finalize();
    // _merger.reset();
    LOGGER_INF << "Query exec finish. " << _requestCount << " dispatched." << std::endl;

    return true;
}

void Executive::remove(int refNum) {
    LOGGER_INF << "Executive::remove(" << refNum << ")\n";
    _unTrack(refNum);
    LOGGER_INF << "FIXME: should check receiver for results\n";
}

void Executive::requestSquash(int refNum) {
    LOGGER_ERR << "Executive::requestSquash() not implemented. Sorry.\n";
}

////////////////////////////////////////////////////////////////////////
// class Executive (private)
////////////////////////////////////////////////////////////////////////
bool Executive::_track(int refNum, ReceiverPtr r) {
    assert(r);
    {
        boost::lock_guard<boost::mutex> lock(_receiversMutex);
        if(_receivers.find(refNum) == _receivers.end()) {
            _receivers[refNum] = r;
        } else {
            return false;
        }
    }
    return true;
}

void Executive::_unTrack(int refNum) {
    {
        boost::lock_guard<boost::mutex> lock(_receiversMutex);
        ReceiverMap::iterator i = _receivers.find(refNum);
        if(i != _receivers.end()) {
            _receivers.erase(i);
            if(_receivers.empty()) _receiversEmpty.notify_all();
        }
    }
}

void Executive::_reapReceivers(boost::unique_lock<boost::mutex> const&) {
    ReceiverMap::iterator i, e;
    while(true) {
        bool reaped = false;
        for(i=_receivers.begin(), e=_receivers.end(); i != e; ++i) {
            if(i->second->getError().msg.empty()) {
                // FIXME do something with the error (msgcode log?)
                _receivers.erase(i);
                reaped = true;
                break;
            }
        }
        if(!reaped) {
            break;
        }
    }
}

void Executive::_waitUntilEmpty() {
    boost::unique_lock<boost::mutex> lock(_receiversMutex);
    int lastCount = -1;
    int count;
    int moreDetailThreshold = 5;
    int complainCount = 0;
    //_printState(LOG_STRM(Debug));
    while(!_receivers.empty()) {
        count = _receivers.size();
        _reapReceivers(lock);
        if(count != lastCount) {
            LOGGER_INF << "Still " << count << " in flight." << std::endl;
            count = lastCount;
            ++complainCount;
            if(complainCount > moreDetailThreshold) {
                _printState(LOG_STRM(Warning));
                complainCount = 0;
            }
        }
        _receiversEmpty.timed_wait(lock, boost::posix_time::seconds(5));
    }
}

/// precondition: _receiversMutex is held by current thread.
void Executive::_printState(std::ostream& os) {
    std::for_each(_receivers.begin(), _receivers.end(),
                  printMapSecond<ReceiverMap::value_type>(os, "\n"));
}

#if 0
// Local Helpers --------------------------------------------------
namespace {

// TODO(smm): These should be created elsewhere, and the thread counts
//            should come from a configuration file.
static DynamicWorkQueue globalReadQueue(25, 5, 50, 0);
static DynamicWorkQueue globalWriteQueue(50, 2, 60, 0);

// Doctors the query path to specify the async path.
// Modifies the string in-place.
void doctorQueryPath(std::string& path) {
    std::string::size_type pos;
    std::string before("/query/");
    std::string after("/query2/");

    pos = path.find(before);
    if(pos != std::string::npos) {
        path.replace(pos, before.size(), after);
    } // Otherwise, don't doctor.
}

}

////////////////////////////////////////////////////////////
// AsyncQueryManager nested classes
////////////////////////////////////////////////////////////

class AsyncQueryManager::printQueryMapValue {
public:
    printQueryMapValue(std::ostream& os_) : os(os_) {}
    void operator()(QueryMap::value_type const& qv) {
        os << "Query with id=" << qv.first;
        os << ": ";
        if(qv.second.first) {
            os << qv.second.first->getDesc();
        } else {
            os << "(NULL)";
        }
        os << ", " << qv.second.second << std::endl;
    }
    std::ostream& os;
};

class AsyncQueryManager::squashQuery {
public:
    squashQuery(boost::mutex& mutex_, QueryMap& queries_)
        :mutex(mutex_), queries(queries_) {}
    void operator()(QueryMap::value_type const& qv) {
        boost::shared_ptr<qdisp::ChunkQuery> cq = qv.second.first;
        if(!cq.get()) return;
        {
            boost::unique_lock<boost::mutex> lock(mutex);
            QueryMap::iterator i = queries.find(qv.first);
            if(i != queries.end()) {
                cq = i->second.first; // Get the shared version.
                if(!cq.get()) {
                    //qv.second.first.reset(); // Erase ours too.
                    return;
                }
                //idInProgress = i->first;
            }
        }
        // Query may have been completed, and its memory freed,
        // but still exist briefly before it is deleted from the map.
        util::Timer t;
        t.start();
        cq->requestSquash();
        t.stop();
        LOGGER_INF << "qSquash " << t << std::endl;
    }
    boost::mutex& mutex;
    QueryMap& queries;
};

////////////////////////////////////////////////////////////
// AsyncQueryManager
////////////////////////////////////////////////////////////

void AsyncQueryManager::finalizeQuery(int id,
                                      xrdc::XrdTransResult r,
                                      bool aborted) {
    std::stringstream ss;
    util::Timer t1;
    t1.start();
    /// Finalize a query.
    /// Note that all parameters should be copies and not const references.
    /// We delete the ChunkQuery (the caller) here, so a ref would be invalid.
    std::string dumpFile;
    std::string tableName;
    int dumpSize;
    LOGGER_DBG << "finalizing. read=" << r.read << " and status is "
               << (aborted ? "ABORTED" : "okay") << std::endl;
    LOGGER_DBG << ((void*)this) << "Finalizing query (" << id << ")" << std::endl;
    if((!aborted) && (r.open >= 0) && (r.queryWrite >= 0)
       && (r.read >= 0)) {
        util::Timer t2;
        t2.start();
        boost::shared_ptr<xrdc::PacketIter> resIter;
        { // Lock scope for reading
            boost::lock_guard<boost::mutex> lock(_queriesMutex);
            QuerySpec& s = _queries[id];
            // Set resIter equal to PacketIter associated with ChunkQuery.
            resIter = s.first->getResultIter();
            dumpFile = s.first->getSavePath();
            dumpSize = s.first->getSaveSize();
            tableName = s.second;
            //assert(r.localWrite == dumpSize); // not valid when using iter
            s.first.reset(); // clear out ChunkQuery.
        }
        // Lock-free merge
        if(resIter) {
            _addNewResult(id, resIter, tableName);
        } else {
            _addNewResult(id, dumpSize, dumpFile, tableName);
        }
        // Erase right before notifying.
        t2.stop();
        ss << id << " QmFinalizeMerge " << t2 << std::endl;
        getMessageStore()->addMessage(id, log::MSG_MERGED, "Results Merged.");
    } // end if
    else {
        util::Timer t2e;
        t2e.start();
        if(!aborted) {
            _isExecFaulty = true;
            LOGGER_INF << "Requesting squash " << id
                       << " because open=" << r.open
                       << " queryWrite=" << r.queryWrite
                       << " read=" << r.read << std::endl;
            _squashExecution();
            LOGGER_INF << " Skipped merge (read failed for id="
                       << id << ")" << std::endl;
        }
        t2e.stop();
        ss << id << " QmFinalizeError " << t2e << std::endl;
    }
    util::Timer t3;
    t3.start();
    {
        boost::lock_guard<boost::mutex> lock(_resultsMutex);
        _results.push_back(Result(id,r));
        if(aborted) ++_squashCount; // Borrow result mutex to protect counter.
        { // Lock again to erase.
            util::Timer t2e1;
            t2e1.start();
            boost::lock_guard<boost::mutex> lock(_queriesMutex);
            _queries.erase(id);
            if(_queries.empty()) _queriesEmpty.notify_all();
            t2e1.stop();
            ss << id << " QmFinalizeErase " << t2e1 << std::endl;
            getMessageStore()->addMessage(id, log::MSG_ERASED,
                                          "Query Resources Erased.");
        }
    }
    t3.stop();
    ss << id << " QmFinalizeResult " << t3 << std::endl;
    LOGGER_DBG << (void*)this << " Done finalizing query (" << id << ")" << std::endl;
    t1.stop();
    ss << id << " QmFinalize " << t1 << std::endl;
    LOGGER_INF << ss.str();
    getMessageStore()->addMessage(id, log::MSG_FINALIZED, "Query Finalized.");
}

// FIXME: With squashing, we should be able to return the result earlier.
// So, clients will call joinResult(), to get the result, and let a reaper
// thread call joinEverything, since that ensures that this object has
// ceased activity and can recycle resources.
// This is a performance optimization.
void AsyncQueryManager::joinEverything() {
    boost::unique_lock<boost::mutex> lock(_queriesMutex);
    int lastCount = -1;
    int count;
    int moreDetailThreshold = 5;
    int complainCount = 0;
    _printState(LOG_STRM(Debug));
    while(!_queries.empty()) {
        count = _queries.size();
        if(count != lastCount) {
            LOGGER_INF << "Still " << count << " in flight." << std::endl;
            count = lastCount;
            ++complainCount;
            if(complainCount > moreDetailThreshold) {
                _printState(LOG_STRM(Warning));
                complainCount = 0;
            }
        }
        _queriesEmpty.timed_wait(lock, boost::posix_time::seconds(5));
    }
    _merger->finalize();
    _merger.reset();
    LOGGER_INF << "Query finish. " << _queryCount << " dispatched." << std::endl;
}

void AsyncQueryManager::configureMerger(rproc::TableMergerConfig const& c) {
    _merger = boost::make_shared<rproc::TableMerger>(c);
}

void AsyncQueryManager::configureMerger(rproc::MergeFixup const& m,
                                        std::string const& resultTable) {
    // Can we configure the merger without involving settings
    // from the python layer? Historically, the Python layer was
    // needed to generate the merging SQL statements, but we are now
    // creating them without Python.
    std::string mysqlBin="obsolete";
    std::string dropMem;
    rproc::TableMergerConfig cfg(_resultDbDb,     // cfg result db
                                  resultTable,     // cfg resultname
                                  m,               // merge fixup obj
                                  _resultDbUser,   // result db credentials
                                  _resultDbSocket, // result db credentials
                                  mysqlBin,        // Obsolete
                                  dropMem          // cfg
                                  );
    _merger = boost::make_shared<rproc::TableMerger>(cfg);
}

std::string AsyncQueryManager::getMergeResultName() const {
    if(_merger.get()) {
        return _merger->getTargetTable();
    }
    return std::string();
}

void AsyncQueryManager::addToReadQueue(DynamicWorkQueue::Callable * callable) {
    globalReadQueue.add(this, callable);
}

void AsyncQueryManager::addToWriteQueue(DynamicWorkQueue::Callable * callable) {
    globalWriteQueue.add(this, callable);
}

boost::shared_ptr<qdisp::MessageStore>
AsyncQueryManager::getMessageStore() {
    if (!_messageStore) {
        // Lazy instantiation of MessageStore.
        _messageStore = boost::make_shared<qdisp::MessageStore>();
    }
    return _messageStore;
}

////////////////////////////////////////////////////////////////////////
// private: ////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////


void AsyncQueryManager::_readConfig(std::map<std::string,
                                    std::string> const& cfg) {
    ConfigMap cm(cfg);
    /// localhost:1094 is the most reasonable default, even though it is
    /// the wrong choice for all but small developer installations.
    _xrootdHostPort = cm.get(
        "frontend.xrootd",
        "WARNING! No xrootd spec. Using localhost:1094",
        "localhost:1094");
    _scratchPath =  cm.get(
        "frontend.scratch_path",
        "Error, no scratch path found. Using /tmp.",
        "/tmp");
    // This should be overriden by the installer properly.
    _resultDbSocket =  cm.get(
        "resultdb.unix_socket",
        "Error, resultdb.unix_socket not found. Using /u1/local/mysql.sock.",
        "/u1/local/mysql.sock");
    _resultDbUser =  cm.get(
        "resultdb.user",
        "Error, resultdb.user not found. Using qsmaster.",
        "qsmaster");
    _resultDbDb =  cm.get(
        "resultdb.db",
        "Error, resultdb.db not found. Using qservResult.",
        "qservResult");

    std::string cssTech = cm.get(
        "css.technology",
        "Error, css.technology not found.",
        "invalid");
    std::string cssConn = cm.get(
        "css.connection",
        "Error, css.connection not found.",
        "");
    _initFacade(cssTech, cssConn);

    std::string defaultDb = cm.get(
        "table.defaultdb",
        "Empty table.defaultdb. Using LSST",
        "LSST");
    _qSession->setDefaultDb(defaultDb);
}

void AsyncQueryManager::_initFacade(std::string const& cssTech,
                                    std::string const& cssConn) {
    if (cssTech == "zoo") {
        LOGGER_INF << "Initializing zookeeper-based css, with "
                   << cssConn << std::endl;
        boost::shared_ptr<css::Facade> cssFPtr(
            css::FacadeFactory::createZooFacade(cssConn));
        _qSession.reset(new qproc::QuerySession(cssFPtr));
    } else if (cssTech == "mem") {
        LOGGER_INF << "Initializing memory-based css, with "
                   << cssConn << std::endl;
        boost::shared_ptr<css::Facade> cssFPtr(
            css::FacadeFactory::createMemFacade(cssConn));
        _qSession.reset(new qproc::QuerySession(cssFPtr));
    } else {
        LOGGER_ERR << "Unable to determine css technology, check config file."
                   << std::endl;
        throw ConfigError("Invalid css technology, check config file.");
    }
}

void AsyncQueryManager::_addNewResult(int id, PacIterPtr pacIter,
                                      std::string const& tableName) {
    LOGGER_DBG << "EXECUTING AsyncQueryManager::_addNewResult(" << id
               << ", pacIter, " << tableName << ")" << std::endl;
    bool mergeResult = _merger->merge(pacIter, tableName);
    ssize_t sz = pacIter->getTotalSize();
    {
        boost::lock_guard<boost::mutex> lock(_totalSizeMutex);
        _totalSize += sz;
    }

    if(_shouldLimitResult && (_totalSize > _resultLimit)) {
        _squashRemaining();
    }
    if(!mergeResult) {
        rproc::TableMergerError e = _merger->getError();
        getMessageStore()->addMessage(id, e.errorCode != 0 ? -abs(e.errorCode) : -1,
                                      "Failed to merge results.");
        if(e.resultTooBig()) {
            _squashRemaining();
        }
    }
}

void AsyncQueryManager::_addNewResult(int id, ssize_t dumpSize,
                                      std::string const& dumpFile,
                                      std::string const& tableName) {
    if(dumpSize < 0) {
        throw std::invalid_argument("dumpSize < 0");
    }
    {
        boost::lock_guard<boost::mutex> lock(_totalSizeMutex);
        _totalSize += dumpSize;
    }

    if(_shouldLimitResult && (_totalSize > _resultLimit)) {
        _squashRemaining();
    }

    if(dumpSize > 0) {
        bool mergeResult = _merger->merge(dumpFile, tableName);
        int res = unlink(dumpFile.c_str()); // Hurry and delete dump file.
        if(0 != res) {
            LOGGER_ERR << "Error removing dumpFile " << dumpFile
                       << " errno=" << errno << std::endl;
        }
        if(!mergeResult) {
            rproc::TableMergerError e = _merger->getError();
            getMessageStore()->addMessage(id, e.errorCode != 0 ? -abs(e.errorCode) : -1,
                                          "Failed to merge results.");
            if(e.resultTooBig()) {
                _squashRemaining();
            }
        }
        LOGGER_DBG << "Merge of " << dumpFile << " into "
                   << tableName
                   << (mergeResult ? " OK----" : " FAIL====")
                   << std::endl;
    }
}


void AsyncQueryManager::_squashExecution() {
    // Halt new query dispatches and cancel the ones in flight.
    // This attempts to save on resources and latency, once a query
    // fault is detected.

    if(_isSquashed) return;
    _isSquashed = true; // Mark before acquiring lock--faster.
    LOGGER_DBG << "Squash requested by "<<(void*)this << std::endl;
    util::Timer t;
    // Squashing is dependent on network latency and remote worker
    // responsiveness, so make a copy so others don't have to wait.
    std::vector<std::pair<int, QuerySpec> > myQueries;
    {
        boost::unique_lock<boost::mutex> lock(_queriesMutex);
        t.start();
        myQueries.resize(_queries.size());
        LOGGER_INF << "AsyncQM squashExec copy " <<  std::endl;
        std::copy(_queries.begin(), _queries.end(), myQueries.begin());
    }
    LOGGER_INF << "AsyncQM squashQueued" << std::endl;
    globalWriteQueue.cancelQueued(this);
    LOGGER_INF << "AsyncQM squashExec iteration " <<  std::endl;
    std::for_each(myQueries.begin(), myQueries.end(),
                  squashQuery(_queriesMutex, _queries));
    t.stop();
    LOGGER_INF << "AsyncQM squashExec " << t << std::endl;
    _isSquashed = true; // Ensure that flag wasn't trampled.

    getMessageStore()->addMessage(-1, log::MSG_EXEC_SQUASHED,
                                  "Query Execution Squashed.");
}

void AsyncQueryManager::_squashRemaining() {
    _squashExecution();  // Not sure if this is right. FIXME
}

#include "XrdSsi/XrdSsiService.hh" // Resource
//#include "XrdSsi/XrdSsiErrInfo.hh"
#include "XrdSsi/XrdSsiRequest.hh"
//#include "XrdSsi/XrdSsiSession.hh"
//#include "XrdSsi/XrdSsiStream.hh"

#include <iostream>
#include <cassert>
#include <string>
#include <unistd.h>
using std::cout;
using std::endl;

namespace {
// Host and port, not url
static char gUrl[] = "localhost:1094";
static std::string requestData1("My request text");
//static char buffer[100];
//static int bufferSize = sizeof(*buffer);

class MyRequest : public XrdSsiRequest {
public:
    MyRequest(XrdSsiSession* session)
        : _bufLen(1000),
          _session(session) {
        _buffer = new char[_bufLen];
        cout << "new _buffer: " << _bufLen << endl;
        _cursor = _buffer;
        _bufRemain = _bufLen;
        _requestDataSize = requestData1.size();
        _requestData = new char[_requestDataSize];
        cout << "new _requestData: " << _requestDataSize << endl;
        memcpy(_requestData, requestData1.data(), _requestDataSize);

    };
    virtual ~MyRequest() {
        _unprovisionSession();
        if(_requestData) { delete[] _requestData; }
        if(_buffer) { delete[] _buffer; }
    }

    // content of request data
    virtual char* GetRequest(int& requestLength) {
        requestLength = _requestDataSize;
        cout << "Requesting [" << requestLength << "] "
             << std::string(_requestData, _requestDataSize) << endl;
        return _requestData;
    }
    virtual void RelRequestBuffer() {
        cout << "Early release of request buffer\n";
        char* data = _requestData;
        _requestData = 0;
        delete[] _requestData;
    }
    // precondition: rInfo.rType != isNone
    virtual bool ProcessResponse(XrdSsiRespInfo const& rInfo, bool isOk) {
        if(!isOk) {
            cout << "Request failed\n";
            _errorFinish();
            return true;
        }
        cout << "Response type is " << rInfo.State() << endl;
        switch(rInfo.rType) {
        case XrdSsiRespInfo::isNone:
            break;
        case XrdSsiRespInfo::isData: // Local-only
            break;
        case XrdSsiRespInfo::isError: // isOk == true
            break;
        case XrdSsiRespInfo::isFile: // Local-only
            break;
        case XrdSsiRespInfo::isStream:
            break;
        }
        // _stream->SetBuff(buf, buflen); // Step 6
        bool retrieveInitiated = GetResponseData(_cursor, _bufRemain); // Step 6
        cout << "Initiated request " << (retrieveInitiated ? "ok" : "err")
             << endl;
        if(!retrieveInitiated) {
            bool ok = Finished();
            if(!ok) cout << "couldn't get response, or clean up request\n";
            else cout << "couldn't get response, but cleanup ok.\n";
            delete this;
            return true;
        } else {
            return true;
        }

    }
    virtual void ProcessResponseData(char *buff, int blen, bool last) { // Step 7
        cout << "ProcessResponse[data] with buff=";
        if(blen < 0) { // error, check errinfo object.
            int eCode;
            cout << " Got an error, eInfo=<" << eInfo.Get(eCode)
                 << ">" << endl;
            _errorFinish();
            return;
        }
        if(blen > 0) {
            cout << std::string(buff, blen) << " [len=" << blen <<"]";
            _cursor = _cursor + blen;
            _bufRemain = _bufRemain - blen;
            if(!last) {
                bool askAgainOk = GetResponseData(_cursor, _bufRemain);
                if(!askAgainOk) {
                    _errorFinish();
                    return;
                }
            }
        }
        if(last || (blen == 0)) {
            cout << "all things received, size=" << _bufLen - _bufRemain
                 << endl;
            _flush();
            _finish();
        } else {
            cout << "(error) len=" << blen;
        }
        cout << (last ? "last=true" : "last=false");
        cout << endl;
    }
    void _flush() {
        std::string block(_buffer, _bufLen - _bufRemain);
        cout << "Handling received block: " << block << endl;
        _cursor = _buffer;
        _bufRemain = _bufLen;
    }
    inline void _unprovisionSession() {
        if(_session) {
            bool ok = _session->Unprovision();
            if(!ok) cout << "Error unprovisioning\n";
            cout << "Unprovision ok.\n";
        }
    }
    void _errorFinish() {
        cout << "Error finish" << endl;
        bool ok = Finished();
        if(!ok) cout << "Error cleaning up MyRequest\n";
        else cout << "Request::Finished() ok.\n";
        delete this; // ???
    }
    void _finish() {
        bool ok = Finished();
        if(!ok) cout << "Error with Finished()\n";
        else cout << "Finished() ok.\n";
        delete this; // ???
    }
    XrdSsiSession* _session;

    XrdSsiStream* _stream; // inherited
    char* _buffer;
    char* _cursor;
    int _bufLen;
    int _bufRemain;
    char* _requestData;
    int _requestDataSize;

}; // class MyRequest

class MyResource : public XrdSsiService::Resource {
public:
    MyResource() : Resource("/terrence") {
    }
    virtual ~MyResource() {}
    void ProvisionDone(XrdSsiSession* so) { // Step 3
        cout << "Provision done\n";
        if(!so) {
            // Check eInfo in resource for error details
            throw "Null XrdSsiSession* passed for MyResource::ProvisionDone";
        }
        _session = so;
        // do something with the session
        makeRequest();
        // If we are not doing anything else with the session,
        // we can stop it after our requests are complete.
        delete this; // Delete ourselves, nobody needs this resource anymore.
    }
    void makeRequest() {
        MyRequest* req = new MyRequest(_session); // Step 4
        cout << "new MyRequest: " << sizeof(MyRequest) << endl;
        // _session takes ownership of the request.
        bool requestSent = _session->ProcessRequest(req); // Step 4
        if(!requestSent) {
            cout << "ProcessRequest failed. deleting\n";
            delete req; // ??
            // handle error
        }
    }
    XrdSsiSession* _session; // unowned, do not delete.
};

class ClientMain {
public:
    ClientMain() : _mr(0) {}
    ~ClientMain() {
        if(_mr) { delete _mr;}
        if(_service) {_service->Stop(); } // Deletes itself, do not "delete" XrdSsiService
    }

    void operator()() {
        XrdSsiErrInfo eInfo;
        _service = XrdSsiGetClientService(eInfo, gUrl); // Step 1
        if(!_service) figureOutError(eInfo);
        assert(_service);
        // Make sure all output is unbuffered
        //        FILE* out = fdopen(2,"w");
        //        setbuf(out, 0);

        _mr = new MyResource(); // 2
        cout << "new MyResource: " << sizeof(MyResource) << endl;
        bool provisionOk = _service->Provision(_mr);  // 2
        if(!provisionOk) {
            cout << "There was an error provisioning " << __FILE__ << __LINE__ << endl;
            figureOutError(_mr->eInfo);
            delete _mr;
            // handle error
        }
        cout << "Provision was ok\n";
        ::sleep(5);
        cout << "We're out of time. Bailing out." << endl;
        _service->Stop(); // Should release all associated XrdSsiSession
        _service = 0;
        // _mr is not my responsibility: It will delete itself
        _mr = 0;
    }
    void figureOutError(XrdSsiErrInfo & e) {
        int errCode;
        cout << "Got Error " << e.Get(errCode) <<  " Code=" << errCode
             << endl;
    }
    MyResource* _mr;
    XrdSsiService* _service;
};
} // anonymous

int main(int,char**) {
    for(int i=0; i<10; ++i) {
        ClientMain cm;
        cm();
    }

    return 0;
}
#include "XrdSsi/XrdSsiService.hh" // Resource
//#include "XrdSsi/XrdSsiErrInfo.hh"
#include "XrdSsi/XrdSsiRequest.hh"
//#include "XrdSsi/XrdSsiSession.hh"
//#include "XrdSsi/XrdSsiStream.hh"

#include <iostream>
#include <cassert>
#include <string>
#include <unistd.h>
using std::cout;
using std::endl;

namespace {
// Host and port, not url
static char gUrl[] = "localhost:1094";
static std::string requestData1("My request text");
//static char buffer[100];
//static int bufferSize = sizeof(*buffer);

class MyRequest : public XrdSsiRequest {
public:
    MyRequest(XrdSsiSession* session)
        : _bufLen(1000),
          _session(session) {
        _buffer = new char[_bufLen];
        cout << "new _buffer: " << _bufLen << endl;
        _cursor = _buffer;
        _bufRemain = _bufLen;
        _requestDataSize = requestData1.size();
        _requestData = new char[_requestDataSize];
        cout << "new _requestData: " << _requestDataSize << endl;
        memcpy(_requestData, requestData1.data(), _requestDataSize);

    };
    virtual ~MyRequest() {
        _unprovisionSession();
        if(_requestData) { delete[] _requestData; }
        if(_buffer) { delete[] _buffer; }
    }

    // content of request data
    virtual char* GetRequest(int& requestLength) {
        requestLength = _requestDataSize;
        cout << "Requesting [" << requestLength << "] "
             << std::string(_requestData, _requestDataSize) << endl;
        return _requestData;
    }
    virtual void RelRequestBuffer() {
        cout << "Early release of request buffer\n";
        char* data = _requestData;
        _requestData = 0;
        delete[] _requestData;
    }
    // precondition: rInfo.rType != isNone
    virtual bool ProcessResponse(XrdSsiRespInfo const& rInfo, bool isOk) {
        if(!isOk) {
            cout << "Request failed\n";
            _errorFinish();
            return true;
        }
        cout << "Response type is " << rInfo.State() << endl;
        switch(rInfo.rType) {
        case XrdSsiRespInfo::isNone:
            break;
        case XrdSsiRespInfo::isData: // Local-only
            break;
        case XrdSsiRespInfo::isError: // isOk == true
            break;
        case XrdSsiRespInfo::isFile: // Local-only
            break;
        case XrdSsiRespInfo::isStream:
            break;
        }
        // _stream->SetBuff(buf, buflen); // Step 6
        bool retrieveInitiated = GetResponseData(_cursor, _bufRemain); // Step 6
        cout << "Initiated request " << (retrieveInitiated ? "ok" : "err")
             << endl;
        if(!retrieveInitiated) {
            bool ok = Finished();
            if(!ok) cout << "couldn't get response, or clean up request\n";
            else cout << "couldn't get response, but cleanup ok.\n";
            delete this;
            return true;
        } else {
            return true;
        }

    }
    virtual void ProcessResponseData(char *buff, int blen, bool last) { // Step 7
        cout << "ProcessResponse[data] with buff=";
        if(blen < 0) { // error, check errinfo object.
            int eCode;
            cout << " Got an error, eInfo=<" << eInfo.Get(eCode)
                 << ">" << endl;
            _errorFinish();
            return;
        }
        if(blen > 0) {
            cout << std::string(buff, blen) << " [len=" << blen <<"]";
            _cursor = _cursor + blen;
            _bufRemain = _bufRemain - blen;
            if(!last) {
                bool askAgainOk = GetResponseData(_cursor, _bufRemain);
                if(!askAgainOk) {
                    _errorFinish();
                    return;
                }
            }
        }
        if(last || (blen == 0)) {
            cout << "all things received, size=" << _bufLen - _bufRemain
                 << endl;
            _flush();
            _finish();
        } else {
            cout << "(error) len=" << blen;
        }
        cout << (last ? "last=true" : "last=false");
        cout << endl;
    }
    void _flush() {
        std::string block(_buffer, _bufLen - _bufRemain);
        cout << "Handling received block: " << block << endl;
        _cursor = _buffer;
        _bufRemain = _bufLen;
    }
    inline void _unprovisionSession() {
        if(_session) {
            bool ok = _session->Unprovision();
            if(!ok) cout << "Error unprovisioning\n";
            cout << "Unprovision ok.\n";
        }
    }
    void _errorFinish() {
        cout << "Error finish" << endl;
        bool ok = Finished();
        if(!ok) cout << "Error cleaning up MyRequest\n";
        else cout << "Request::Finished() ok.\n";
        delete this; // ???
    }
    void _finish() {
        bool ok = Finished();
        if(!ok) cout << "Error with Finished()\n";
        else cout << "Finished() ok.\n";
        delete this; // ???
    }
    XrdSsiSession* _session;

    XrdSsiStream* _stream; // inherited
    char* _buffer;
    char* _cursor;
    int _bufLen;
    int _bufRemain;
    char* _requestData;
    int _requestDataSize;

}; // class MyRequest

class MyResource : public XrdSsiService::Resource {
public:
    MyResource() : Resource("/terrence") {
    }
    virtual ~MyResource() {}
    void ProvisionDone(XrdSsiSession* so) { // Step 3
        cout << "Provision done\n";
        if(!so) {
            // Check eInfo in resource for error details
            throw "Null XrdSsiSession* passed for MyResource::ProvisionDone";
        }
        _session = so;
        // do something with the session
        makeRequest();
        // If we are not doing anything else with the session,
        // we can stop it after our requests are complete.
        delete this; // Delete ourselves, nobody needs this resource anymore.
    }
    void makeRequest() {
        MyRequest* req = new MyRequest(_session); // Step 4
        cout << "new MyRequest: " << sizeof(MyRequest) << endl;
        // _session takes ownership of the request.
        bool requestSent = _session->ProcessRequest(req); // Step 4
        if(!requestSent) {
            cout << "ProcessRequest failed. deleting\n";
            delete req; // ??
            // handle error
        }
    }
    XrdSsiSession* _session; // unowned, do not delete.
};

class ClientMain {
public:
    ClientMain() : _mr(0) {}
    ~ClientMain() {
        if(_mr) { delete _mr;}
        if(_service) {_service->Stop(); } // Deletes itself, do not "delete" XrdSsiService
    }

    void operator()() {
        XrdSsiErrInfo eInfo;
        _service = XrdSsiGetClientService(eInfo, gUrl); // Step 1
        if(!_service) figureOutError(eInfo);
        assert(_service);
        // Make sure all output is unbuffered
        //        FILE* out = fdopen(2,"w");
        //        setbuf(out, 0);

        _mr = new MyResource(); // 2
        cout << "new MyResource: " << sizeof(MyResource) << endl;
        bool provisionOk = _service->Provision(_mr);  // 2
        if(!provisionOk) {
            cout << "There was an error provisioning " << __FILE__ << __LINE__ << endl;
            figureOutError(_mr->eInfo);
            delete _mr;
            // handle error
        }
        cout << "Provision was ok\n";
        ::sleep(5);
        cout << "We're out of time. Bailing out." << endl;
        _service->Stop(); // Should release all associated XrdSsiSession
        _service = 0;
        // _mr is not my responsibility: It will delete itself
        _mr = 0;
    }
    void figureOutError(XrdSsiErrInfo & e) {
        int errCode;
        cout << "Got Error " << e.Get(errCode) <<  " Code=" << errCode
             << endl;
    }
    MyResource* _mr;
    XrdSsiService* _service;
};
} // anonymous

int main(int,char**) {
    for(int i=0; i<10; ++i) {
        ClientMain cm;
        cm();
    }

    return 0;
}

#endif
}}} // namespace lsst::qserv::qdisp

