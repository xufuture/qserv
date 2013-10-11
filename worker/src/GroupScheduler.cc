/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
  * @file GroupScheduler.cc
  *
  * @brief A scheduler implementation that limits disk scans to one at
  * a time, but allows multiple queries to share I/O.
  *
  * @author Daniel L. Wang, SLAC
  */
#include "lsst/qserv/worker/GroupScheduler.h"
#include <iostream>
#include <sstream>
#include <boost/thread.hpp>
#include "lsst/qserv/worker/Logger.h"

namespace lsst {
namespace qserv {
namespace worker {

////////////////////////////////////////////////////////////////////////
// class GroupWatcher
// Lets the scheduler listen to a Foreman's Runners.
////////////////////////////////////////////////////////////////////////
class GroupWatcher : public Foreman::RunnerWatcher {
public:

    GroupWatcher(GroupScheduler& gs, boost::mutex& mutex)
        : _gs(gs), _mutex(mutex) {}
    virtual void handleStart(Task::Ptr t) {
        boost::lock_guard<boost::mutex> guard(_mutex);
        // FIXME!!!
    }
    virtual void handleFinish(Task::Ptr t) {
        boost::lock_guard<boost::mutex> guard(_mutex);
        // FIXME!!!
    }
private:
    GroupScheduler& _gs;
    boost::mutex& _mutex;
};

////////////////////////////////////////////////////////////////////////
// class GroupScheduler
////////////////////////////////////////////////////////////////////////
GroupScheduler::GroupScheduler(Logger::Ptr logger)
    : _maxRunning(4), // FIXME: set to some multiple of system proc count.
      _logger(logger)
{
}

void GroupScheduler::queueTaskAct(Task::Ptr incoming) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    _enqueueTask(incoming);
}

TaskQueuePtr GroupScheduler::nopAct(TaskQueuePtr running) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    assert(_integrityHelper());
    return _getNextIfAvail(running->size());
}

/// @return a queue of all tasks ready to run.
///
TaskQueuePtr GroupScheduler::newTaskAct(Task::Ptr incoming,
                                       TaskQueuePtr running) {
    boost::lock_guard<boost::mutex> guard(_mutex);
    assert(_integrityHelper());
    assert(running.get());
    _enqueueTask(incoming);
    return _getNextIfAvail(running->size());
}

TaskQueuePtr GroupScheduler::taskFinishAct(Task::Ptr finished,
                                          TaskQueuePtr running) {

    boost::lock_guard<boost::mutex> guard(_mutex);
    assert(_integrityHelper());

    std::ostringstream os;
    os << "Completed: " << "(" << finished->msg->chunkid()
       << ")" << finished->msg->fragment(0).query(0);
    _logger->debug(os.str());
    return _getNextIfAvail(running->size());
}

boost::shared_ptr<Foreman::RunnerWatcher> GroupScheduler::getWatcher() {
    boost::shared_ptr<GroupWatcher> w;
    w.reset(new GroupWatcher(*this, _mutex));
    return w;
}

/// @return true if data is okay.
bool GroupScheduler::checkIntegrity() {
    boost::lock_guard<boost::mutex> guard(_mutex);
    return _integrityHelper();
}

/// @return true if data is okay
/// precondition: _mutex is locked.
bool GroupScheduler::_integrityHelper() {
    // FIXME
    return true;
}

/// Precondition: _mutex is already locked.
/// @return new tasks to run
/// TODO: preferential treatment for chunkId just run?
/// or chunkId that are currently running?
TaskQueuePtr GroupScheduler::_getNextIfAvail(int runCount) {
    int available = _maxRunning - runCount;
    if(available <= 0) {
        return TaskQueuePtr();
    }
    return _getNextTasks(available);
}

/// Precondition: _mutex is already locked.
/// @return new tasks to run
TaskQueuePtr GroupScheduler::_getNextTasks(int max) {
    // FIXME: Select disk based on chunk location.
    if(max < 1) { throw std::invalid_argument("max < 1)"); }
    std::ostringstream os;
    os << "_getNextTasks(" << max << ")>->->";
    _logger->debug(os.str());
    os.str("");
    TaskQueuePtr tq;
    if(_queue.size() > 0) {
        tq.reset(new TaskQueue());
        for(int i=max; i >= 0; --i) {
            if(_queue.empty()) { break; }
            Task::Ptr t = _queue.front();
            tq->push_back(t);
            _queue.pop_front();
        }
    }
    if(tq) {
        os << "Returning " << tq->size() << " to launch";
        _logger->debug(os.str());
    }
    assert(_integrityHelper());
    _logger->debug("_getNextTasks <<<<<");
    return tq;
}

/// Precondition: _mutex is locked.
void GroupScheduler::_enqueueTask(Task::Ptr incoming) {
    if(!incoming) {
        throw std::invalid_argument("null task");
    }
    _queue.insert(incoming);
    std::ostringstream os;
    TaskMsg const& msg = *(incoming->msg);
    os << "Adding new task: " << msg.chunkid()
       << " : " << msg.fragment(0).query(0);
    _logger->debug(os.str());
}

}}} // lsst::qserv::worker
