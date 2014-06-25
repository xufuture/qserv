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
#include <algorithm>
#include <iostream>
#include <sstream>

// Third-party headers
#include "XrdSsi/XrdSsiErrInfo.hh"
#include "XrdSsi/XrdSsiService.hh" // Resource
#include "XrdOuc/XrdOucTrace.hh"

// #include "boost/date_time/posix_time/posix_time_types.hpp"
// #include <boost/format.hpp>
// #include <boost/make_shared.hpp>

// // Local headers
// #include "ccontrol/ConfigMap.h"
// #include "css/Facade.h"
//#include "global/stringTypes.h"
#include "global/ResourceUnit.h"
#include "log/Logger.h"
#include "log/msgCode.h"
// #include "rproc/TableMerger.h"
// #include "qdisp/ChunkQuery.h"
#include "qdisp/MessageStore.h"
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
void populateState(lsst::qserv::qdisp::ExecStatus& es,
                   lsst::qserv::qdisp::ExecStatus::State s,
                   XrdSsiErrInfo& e) {
    int code;
    std::string desc(e.Get(code));
    es.report(s, code, desc);
}
} // anonymous namespace

// Declare to allow force-on XrdSsi tracing
#define TRACE_ALL       0xffff
#define TRACE_Debug     0x0001
namespace XrdSsi {
extern XrdOucTrace     Trace;
}

//#define LOGGER_INF std::cout
//#define LOGGER_ERR std::cout
//#define LOGGER_WRN std::cout
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

////////////////////////////////////////////////////////////////////////
// class Executive implementation
////////////////////////////////////////////////////////////////////////
Executive::Executive(Config::Ptr c, boost::shared_ptr<MessageStore> ms)
    : _config(*c),
      _messageStore(ms) {
    _setup();
}

void Executive::_setup() {

    XrdSsi::Trace.What = TRACE_ALL | TRACE_Debug;

    XrdSsiErrInfo eInfo;
    _service = XrdSsiGetClientService(eInfo, _config.serviceUrl.c_str()); // Step 1
    if(!_service) figureOutError(eInfo);
    assert(_service);
    _requestCount = 0;
}

void Executive::add(int refNum, TransactionSpec const& t,
                    std::string const& resultName) {
    LOGGER_INF << "EXECUTING Executive::add(TransactionSpec, "
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
    LOGGER_INF << "Trying to cancel all queries...\n";
    std::vector<int> pending;
    {
        boost::lock_guard<boost::mutex> lock(_receiversMutex);
        std::ostringstream os;
        os << "STATE=";
        _printState(os);
        LOGGER_INF << os.str() << std::endl
                   << "Loop cancel all queries...\n";
        ReceiverMap::iterator i,e;
        for(i=_receivers.begin(), e=_receivers.end(); i != e; ++i) {
            i->second->cancel();
            pending.push_back(i->first);
        }
        LOGGER_INF << "Loop cancel all queries...done\n";
    }
    { // Should be possible to convert this to a for_each call.
        std::vector<int>::iterator i,e;
        for(i=pending.begin(), e=pending.end(); i != e; ++i) {
            _unTrack(*i);
        }
    }
}

// Add a spec to be executed. Not thread-safe.
void Executive::add(int refNum, Executive::Spec const& s) {
    bool trackOk =_track(refNum, s.receiver); // Remember so we can join.
    if(!trackOk) {
        LOGGER_WRN << "Ignoring duplicate add(" << refNum << ")\n";
        return;
    }
    ExecStatus& status = _insertNewStatus(refNum, s.resource);
    ++_requestCount;
    std::string msg = "Exec add pth=" + s.resource.path();
    LOGGER_INF << msg << std::endl;
    _messageStore->addMessage(s.resource.chunk(), log::MSG_MGR_ADD, msg);

    QueryResource* r = new QueryResource(s.resource.path(),
                                         s.request,
                                         s.receiver,
                                         status);
    status.report(ExecStatus::PROVISION);
    bool provisionOk = _service->Provision(r);  // 2
    if(!provisionOk) {
        LOGGER_ERR << "Resource provision error " << s.resource.path()
                   << std::endl;
        populateState(status, ExecStatus::PROVISION_ERROR, r->eInfo);
        _unTrack(refNum);
        delete r;

        // handle error
    }
    LOGGER_DBG << "Provision was ok\n";
}

bool Executive::join() {
    // To join, we make sure that all of the chunks added so far are complete.
    // Check to see if _receivers is empty, if not, then sleep on a condition.
    _waitUntilEmpty();
    // Okay to merge. probably not the Executive's responsibility
    // _merger->finalize();
    // _merger.reset();
    struct successF {
        static bool f(Executive::StatusMap::value_type const& entry) {
            ExecStatus const& es = *entry.second;
            return es.state == ExecStatus::RESPONSE_DONE; } };
    int sCount = std::count_if(_statuses.begin(), _statuses.end(), successF::f);

    LOGGER_INF << "Query exec finish. " << _requestCount << " dispatched." << std::endl;

    return sCount == _requestCount;
}

void Executive::markCompleted(int refNum, bool success) {
    QueryReceiver::Error e;
    LOGGER_INF << "Executive::markCompletion(" << refNum << ","
               << success  << ")\n";
    if(!success) {
        boost::lock_guard<boost::mutex> lock(_receiversMutex);
        ReceiverMap::iterator i = _receivers.find(refNum);
        if(i != _receivers.end()) {
            e = i->second->getError();
        } else {
            LOGGER_ERR << "Executive(" << (void*)this << ")"
                       << "Failed to find tracked id="
                       << refNum << " size=" << _receivers.size()
                       << std::endl;
            assert(false);
        }
        _statuses[refNum]->report(ExecStatus::RESULT_ERROR, 1);
        LOGGER_ERR << "Executive: error executing refnum=" << refNum << "."
                   << " code=" << e.code << " " << e.msg << std::endl;
    }
    _unTrack(refNum);
    if(!success) {
        LOGGER_ERR << "Executive: requesting squash (cause refnum="
                   << refNum << " with "
                   << " code=" << e.code << " " << e.msg << std::endl;
        abort(); // ask to abort
    }
}

void Executive::requestSquash(int refNum) {
    LOGGER_ERR << "Executive::requestSquash() not implemented. Sorry.\n";
}

struct printMapEntry {
    printMapEntry(std::ostream& os_, std::string const& sep_)
        : os(os_), sep(sep_), first(true) {}
    void operator()(Executive::StatusMap::value_type const& entry) {
        if(!first) { os << sep; }
        os << "Ref=" << entry.first << " ";
        ExecStatus const& es = *entry.second;
        os << es;
        first = false;
    }
    std::ostream& os;
    std::string const& sep;
    bool first;
};

std::string Executive::getProgressDesc() const {
    std::ostringstream os;
    std::for_each(_statuses.begin(), _statuses.end(), printMapEntry(os, "\n"));
    LOGGER_ERR << os.str() << std::endl;
    return os.str();
}

////////////////////////////////////////////////////////////////////////
// class Executive (private)
////////////////////////////////////////////////////////////////////////
ExecStatus& Executive::_insertNewStatus(int refNum,
                                        ResourceUnit const& r) {
    ExecStatus::Ptr es(new ExecStatus(r));
    _statuses.insert(StatusMap::value_type(refNum, es));
    return *es;
}

bool Executive::_track(int refNum, ReceiverPtr r) {
    assert(r);
    {
        boost::lock_guard<boost::mutex> lock(_receiversMutex);
        // LOGGER_INF
        LOGGER_DBG << "Executive (" << (void*)this << ") tracking  id="
                   << refNum << std::endl;
        if(_receivers.find(refNum) == _receivers.end()) {
            _receivers[refNum] = r;
        } else {
            return false;
        }
    }
    return true;
}

void Executive::_unTrack(int refNum) {
    boost::lock_guard<boost::mutex> lock(_receiversMutex);
    ReceiverMap::iterator i = _receivers.find(refNum);
    if(i != _receivers.end()) {
        LOGGER_INF << "Executive (" << (void*)this << ") UNTRACKING  id="
                   << refNum << std::endl;
        _receivers.erase(i);
        if(_receivers.empty()) _receiversEmpty.notify_all();
    }
}

void Executive::_reapReceivers(boost::unique_lock<boost::mutex> const&) {
    ReceiverMap::iterator i, e;
    while(true) {
        bool reaped = false;
        for(i=_receivers.begin(), e=_receivers.end(); i != e; ++i) {
            if(!i->second->getError().msg.empty()) {
                // FIXME do something with the error (msgcode log?)
                LOGGER_INF << "Executive (" << (void*)this << ") REAPED  id="
                   << i->first << std::endl;

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
    os << std::endl << getProgressDesc() << std::endl;
}

#if 0
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

#endif
}}} // namespace lsst::qserv::qdisp

