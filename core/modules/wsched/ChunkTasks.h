// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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
#ifndef LSST_QSERV_WSCHED_CHUNKTASKS_H
#define LSST_QSERV_WSCHED_CHUNKTASKS_H

// System headers
#include <memory>
#include <set>
#include <vector>

// Qserv headers
#include "memman/MemMan.h"
#include "wbase/Task.h"
#include "wsched/SlowTableHeap.h"

namespace lsst {
namespace qserv {
namespace wsched {

/// A class to store Tasks for a specific chunk.
/// Tasks are normally placed on _activeTasks, but will be added
/// to _pendingTasks when this is the active chunk.
/// The active chunk is the first chunk to be checked for tasks to run.
/// Placing tasks on the pending list prevents getting stuck on the
/// active chunk indefinitely.
class ChunkTasks {
public:
    using Ptr = std::shared_ptr<ChunkTasks>;
    enum class ReadyState {READY, NOT_READY, NO_RESOURCES};

    ChunkTasks(int chunkId, memman::MemMan::Ptr const& memMan) : _chunkId{chunkId}, _memMan{memMan} {}
    ChunkTasks() = delete;
    ChunkTasks(ChunkTasks const&) = delete;
    ChunkTasks& operator=(ChunkTasks const&) = delete;

    bool empty() const;
    void queTask(wbase::Task::Ptr const& task);
    wbase::Task::Ptr getTask(bool useFlexibleLock);
    ReadyState ready(bool useFlexibleLock);
    void taskComplete(wbase::Task::Ptr const& task);

    void movePendingToActive(); ///< Move all pending Tasks to _activeTasks.
    bool readyToAdvance(); ///< @return true if active Tasks for this chunk are done.
    void setActive(bool active=true); ///< Flag current requests so new requests will be pending.
    bool setResourceStarved(bool starved); ///< hook for tracking starvation.
    std::size_t size() const { return _activeTasks.size() + _pendingTasks.size(); }
    int getChunkId() { return _chunkId; }

    wbase::Task::Ptr removeTask(wbase::Task::Ptr const& task);

private:

    int _chunkId;                    ///< Chunk Id for all Tasks in this instance.
    bool _active{false};            ///< True when this is the active chunk.
    bool _resourceStarved{false};   ///< True when advancement is prevented by lack of memory.
    wbase::Task::Ptr              _readyTask{nullptr}; ///< Task that is ready to run with memory reserved.
    SlowTableHeap                 _activeTasks;        ///< All Tasks must be put on this before they can run.
    std::vector<wbase::Task::Ptr> _pendingTasks;       ///< Task that should not be run until later.
    std::set<wbase::Task*>        _inFlightTasks;      ///< Set of Tasks that this chunk has in flight.

    memman::MemMan::Ptr _memMan;
};

}}} // namespace lsst::qserv::wsched

#endif // LSST_QSERV_WSCHED_CHUNKTASKS_H
