/*
 * LSST Data Management System
 * Copyright 2015-2016 AURA/LSST.
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

// System headers
#include <sstream>

// Third-party headers

// Class header
#include "qdisp/JobQuery.h"

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qdisp/Executive.h"
#include "qdisp/QueryRequest.h"

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qdisp.JobQuery");
} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qdisp {


JobQuery::JobQuery(Executive::Ptr const& executive, JobDescription const& jobDescription,
                   JobStatus::Ptr const& jobStatus,
                   std::shared_ptr<MarkCompleteFunc> const& markCompleteFunc,
                   QueryId qid) :
  _executive(executive), _jobDescription(jobDescription),
  _markCompleteFunc(markCompleteFunc), _jobStatus(jobStatus),
  _qid{qid},
  _idStr{QueryIdHelper::makeIdStr(qid, getIdInt())} {
      _largeResultMgr = executive->getLargeResultMgr();
    LOGS(_log, LOG_LVL_DEBUG, "JobQuery " << getIdStr() << " desc=" << _jobDescription);
  std::cerr <<"JobQuery alloc@" <<std::hex<<this<<std::dec<<'\n'<<std::flush;
}

JobQuery::~JobQuery() {
    LOGS(_log, LOG_LVL_DEBUG, "~JobQuery " << getIdStr());
  std::cerr <<pthread_self()<<" JobQuery dstry@" <<std::hex<<this<<std::dec<<'\n'<<std::flush;
}

/** Attempt to run the job on a worker.
 * @return - false if it can not setup the job or the maximum number of retries has been reached.
 */
bool JobQuery::runJob() {
    LOGS(_log, LOG_LVL_DEBUG, "runJob " << *this);
    auto executive = _executive.lock();
    if (executive == nullptr) {
        LOGS(_log, LOG_LVL_ERROR, getIdStr() << "runJob failed executive==nullptr");
        return false;
    }
    bool cancelled = executive->getCancelled();
    bool handlerReset = _jobDescription.respHandler()->reset();

    // If we haven't been cancelled and the request handler is clean, tell the
    // executive to start the query. We must do this under the control of
    // _rmutex in order to atomically track where we are relative to SSI for
    // cancellation purposes.
    if (!cancelled && handlerReset) {
        std::shared_ptr<JobQuery> jq(shared_from_this());
        std::lock_guard<std::recursive_mutex> lock(_rmutex);
        if ( _runAttemptsCount < _getMaxRetries() ) {
            ++_runAttemptsCount;
            _inSSI = true;
            if (executive->StartQuery(jq)) {
               _jobStatus->updateInfo(JobStatus::REQUEST);
               return true;
            } else _inSSI = false;
        } else {
            LOGS(_log, LOG_LVL_ERROR, getIdStr() << " hit maximum number of retries ("
                 << _runAttemptsCount << ") Canceling user query!");
            executive->squash(); // This should kill all jobs in this user query.
            return false;
        }
    }

    LOGS_WARN(getIdStr() << " JobQuery Failed to RunJob failed. cancelled=" << cancelled
              << " reset=" << handlerReset);
    return false;
}

/// Cancel response handling. Return true if this is the first time cancel has been called.
bool JobQuery::cancel() {
    LOGS_DEBUG(getIdStr() << " JobQuery::cancel()");
    if (_cancelled.exchange(true) == false) {
        std::lock_guard<std::recursive_mutex> lock(_rmutex);
        // If _inSSI is true, then this job has been passed to SSI and
        // cancellation is complicated.
        if (_inSSI) {
            LOGS_DEBUG(getIdStr() << " cancel QueryRequest in progress");
            _queryRequestPtr->cancel();
        } else {
            std::ostringstream os;
            os << getIdStr() <<" cancel before QueryRequest" ;
            LOGS_DEBUG(os.str());
            getDescription().respHandler()->errorFlush(os.str(), -1);
            auto executive = _executive.lock();
            if (executive == nullptr) {
                LOGS(_log, LOG_LVL_ERROR, " can't markComplete cancelled, executive == nullptr");
                return false;
            }
            executive->markCompleted(getIdInt(), false);
        }
        _jobDescription.respHandler()->processCancel();
        return true;
    }
    LOGS_DEBUG(getIdStr() << " cancel, skipping, already cancelled.");
    return false;
}


/// @return true if this job's executive has been cancelled.
/// There is enough delay between the executive being cancelled and the executive
/// cancelling all the jobs that it makes a difference. If either the executive,
/// or the job has cancelled, proceeding is probably not a good idea.
bool JobQuery::isQueryCancelled() {
    auto exec = _executive.lock();
    if (exec == nullptr) {
        LOGS_WARN(getIdStr() << " _executive == nullptr");
        return true; // Safer to assume the worst.
    }
    return exec->getCancelled();
}

std::ostream& operator<<(std::ostream& os, JobQuery const& jq) {
    return os << "{" << jq.getIdStr() << jq._jobDescription << " " << *jq._jobStatus << "}";
}


}}} // end namespace
