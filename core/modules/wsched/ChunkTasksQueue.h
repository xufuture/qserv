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
#ifndef LSST_QSERV_WSCHED_CHUNKTASKLIST_H
#define LSST_QSERV_WSCHED_CHUNKTASKLIST_H

// System headers
#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <mutex>

// Qserv headers
#include "memman/MemMan.h"
#include "wbase/Task.h"
#include "wsched/ChunkTaskCollection.h"
#include "wsched/ChunkTasks.h"
#include "wsched/SchedulerBase.h"

namespace lsst {
namespace qserv {
namespace wsched {

/// This class queues Tasks by their chunkId and tables rating and names.
/// New Tasks are queued with other Tasks with the same chunkId and then by shared
/// scan tables used.
/// - Tasks are provided starting with the _activeChunk, which remains the
///   _activeChunk until all of its Tasks are completed. At which time, the
///   _activeChunk advances to the chunk with the next higher chunkId. While
///   a chunk is the _activeChunk, all new Tasks for that chunk are put in
///   in a pending list so that the active chunk does not get stalled.
/// - While all the Tasks on the _active chunk have been started, but not completed,
///   Tasks can be taken from chunks after the _activeChunk as long as resources are
///   available.
/// Like the other schedulers, ready() is the core of this class as it determines
/// if a Task is ready to run and which Task will be provided by getTask().
class ChunkTasksQueue : public ChunkTaskCollection {
public:
    using Ptr = std::shared_ptr<ChunkTasksQueue>;

    /// This must be std::map to maintain valid iterators.
    /// Only erase() will invalidate and iterator with std::map.
    using ChunkMap = std::map<int, ChunkTasks::Ptr>;

    enum {READY, NOT_READY, NO_RESOURCES};

    ChunkTasksQueue(SchedulerBase *scheduler, memman::MemMan::Ptr const& memMan) :
        _memMan{memMan}, _scheduler{scheduler} {}
    ChunkTasksQueue(ChunkTasksQueue const&) = delete;
    ChunkTasksQueue& operator=(ChunkTasksQueue const&) = delete;

    void queueTask(wbase::Task::Ptr const& task) override;
    wbase::Task::Ptr getTask(bool useFlexibleLock) override;
    bool empty() const override;
    std::size_t getSize() const override { return _taskCount; }
    bool ready(bool useFlexibleLock) override;
    void taskComplete(wbase::Task::Ptr const& task) override;

    bool setResourceStarved(bool starved) override;
    bool nextTaskDifferentChunkId() override;
    int getActiveChunkId(); ///< return the active chunk id, or -1 if there isn't one.

    wbase::Task::Ptr removeTask(wbase::Task::Ptr const& task) override;

private:
    bool _ready(bool useFlexibleLock);
    bool _empty() const { return _chunkMap.empty(); }

    mutable std::mutex _mapMx; ///< Protects _chunkMap, _activeChunk, and _readyChunk.
    ChunkMap _chunkMap; ///< map by chunk Id.
    ChunkMap::iterator _activeChunk{_chunkMap.end()}; ///< points at the active ChunkTasks in _chunkList
    ChunkTasks::Ptr _readyChunk{nullptr}; ///< Chunk with the task that's ready to run.

    memman::MemMan::Ptr _memMan;
    std::atomic<int> _taskCount{0}; ///< Count of all tasks currently in _chunkMap.
    bool _resourceStarved{false};
    SchedulerBase* _scheduler; ///< Pointer to scheduler that owns this. This can be nullptr.
};

}}} // namespace lsst::qserv::wsched

#endif // LSST_QSERV_WSCHED_CHUNKTASKLIST_H
