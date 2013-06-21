// -*- LSST-C++ -*-
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
#ifndef LSST_QSERV_WORKER_CHUNKDISK_H
#define LSST_QSERV_WORKER_CHUNKDISK_H
 /**
  * @file ChunkDisk.h  
  *
  * @brief ChunkDisk is a resource that queues tasks for chunks on a disk.
  *
  * @author Daniel L. Wang, SLAC
  */ 
#include <queue>
#include <boost/shared_ptr.hpp>

#include "lsst/qserv/worker/QueryRunnerManager.h" // QueryRunnerArg
namespace lsst { 
namespace qserv { 
namespace worker {
class Task;

class ChunkDisk {
public:
    //typedef QueryRunnerArg Element;
    typedef Task Element;
    typedef boost::shared_ptr<Element> ElementPtr;
    class TaskPtrCompare {
    public:
        bool operator()(ElementPtr const& x, ElementPtr const& y) {
            if(!x || !y) { return false; }
            if((!x->msg) || (!y->msg)) { return false; }
            return x->msg->chunkid()  < y->msg->chunkid();
        }
    };
    typedef std::priority_queue<ElementPtr, std::vector<ElementPtr>, TaskPtrCompare> Queue;
    typedef std::list<ElementPtr> List;

    ChunkDisk() {} // FIXME
    List getInflight() const;

    // Queue management
    void enqueue(ElementPtr a);
    ElementPtr getNext(bool allowAdvance);
    bool busy() const; /// Busy scanning a chunk?
    bool empty() const;

    // Inflight management
    List::iterator registerInflight(ElementPtr const& e);
    void removeInflight(List::iterator i);
        

private:
    mutable boost::mutex _queueMutex;
    Queue _activeTasks;
    Queue _pendingTasks;
    mutable boost::mutex _inflightMutex;
    List _inflight;
    int _currentChunkId; // Last chunkId retrieved from queue
    bool _completed;
};


}}} // lsst::qserv::worker
#endif // LSST_QSERV_WORKER_CHUNKDISK_H

