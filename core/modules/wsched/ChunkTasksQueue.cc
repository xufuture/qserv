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


// Class header
#include "ChunkTasksQueue.h"

// System headers
#include <mutex>
#include <utility>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.wsched.ChunkTasksQueue");
}

namespace lsst {
namespace qserv {
namespace wsched {


/// Queue a Task with other tasks on the same chunk.
void ChunkTasksQueue::queueTask(wbase::Task::Ptr const& task) {
    // Insert a new ChunkTask object into the map if it doesn't already exist.
    auto insertChunkTask = [this](int chunkId) -> ChunkTasksQueue::ChunkMap::iterator {
        auto iter = _chunkMap.find(chunkId);
        if (iter == _chunkMap.end()) {
            std::pair<int, ChunkTasks::Ptr> ele(chunkId, std::make_shared<ChunkTasks>(chunkId, _memMan));
            auto res = _chunkMap.insert(ele); // insert should fail if the key already exists.
            LOGS(_log, LOG_LVL_DEBUG, " queueTask chunk=" << chunkId << " created=" << res.second);
            iter =  res.first;
        }
        return iter;
    };

    std::lock_guard<std::mutex> lg(_mapMx);
    int chunkId = task->getChunkId();
    auto iter = insertChunkTask(chunkId);
    ++_taskCount;
    iter->second->queTask(task);
}


/// @return true if this object is ready to provide a Task from its queue.
bool ChunkTasksQueue::ready(bool useFlexibleLock) {
    std::lock_guard<std::mutex> lock(_mapMx);
    return _ready(useFlexibleLock);
}


/// Precondition: _queueMutex must be locked
/// @return true if this object is ready to provide a Task from its queue with _readyChunk pointing
///         to a chunk with a Task that is ready to run.
///         When returning false, _readyChunk will be nullptr.
/// This function starts checking at the _activeChunk and only looks to the next chunk
/// if there are no tasks to run on the current chunk. It continues through the list until
/// all chunks have been checked, a ready task is found, or there are not enough resources to
/// run the next Task on the current chunk.
/// The _activeChunk advances when all of its Tasks have completed.
bool ChunkTasksQueue::_ready(bool useFlexibleLock) {
    if (_readyChunk != nullptr) {
        return true;
    }
    if (_empty()) {
        return false;
    }

    // If the _activeChunk is invalid, start at the beginning.
    if (_activeChunk == _chunkMap.end()) {
        _activeChunk = _chunkMap.begin();
        _activeChunk->second->setActive(); // Flag tasks on active so new Tasks added wont be run.
    }

    // Check the active chunk for valid Tasks
    if (_activeChunk->second->ready(useFlexibleLock) == ChunkTasks::ReadyState::READY) {
        _readyChunk = _activeChunk->second;
        return true;
    }

    // Should the active chunk be advanced?
    if (_activeChunk->second->readyToAdvance()) {
        auto newActive = _activeChunk;
        ++newActive;
        if (newActive == _chunkMap.end()) {
            newActive = _chunkMap.begin();
        }

        // Clean up the old _active chunk before moving on.
        _activeChunk->second->setActive(false); // This should move pending Tasks to _activeTasks
        // _inFlightTasks must be empty as readyToAdvance was true.
        if (_activeChunk->second->empty()) {
            if (newActive == _activeChunk) {
                newActive = _chunkMap.end();
            }
            _chunkMap.erase(_activeChunk);
        }

        _activeChunk = newActive;
        if (newActive == _chunkMap.end()) {
            // _chunkMap is empty.
            return false;
        }
        newActive->second->movePendingToActive();
        newActive->second->setActive();
    }

    // Advance through chunks until READY or NO_RESOURCES found, or until entire list scanned.
    auto iter = _activeChunk;
    ChunkTasks::ReadyState chunkState = iter->second->ready(useFlexibleLock);
    while (chunkState != ChunkTasks::ReadyState::READY
           && chunkState != ChunkTasks::ReadyState::NO_RESOURCES) {
        ++iter;
        if (iter == _chunkMap.end()) {
            iter = _chunkMap.begin();
        }
        if (iter == _activeChunk) {
            return false;
        }
        if (_scheduler != nullptr
              && _scheduler->getActiveChunkCount() >= _scheduler->getMaxActiveChunks()) {
            int newChunkId = iter->second->getChunkId();
            if (!_scheduler->chunkAlreadyActive(newChunkId)) {
                return false;
            }
        }

        chunkState = iter->second->ready(useFlexibleLock);
    }
    if (chunkState == ChunkTasks::ReadyState::NO_RESOURCES) {
        // Advancing past a chunk where there aren't enough resources could cause many
        // scheduling issues.
        return false;
    }
    _readyChunk = iter->second;
    return true;
}


wbase::Task::Ptr ChunkTasksQueue::getTask(bool useFlexibleLock) {
    std::lock_guard<std::mutex> lock(_mapMx);
    // Attempt to set _readyChunk.
    _ready(useFlexibleLock);
    // If a Task was ready, _readyChunk will not be nullptr.
    if (_readyChunk != nullptr) {
        wbase::Task::Ptr task = _readyChunk->getTask(useFlexibleLock);
        _readyChunk = nullptr;
        --_taskCount;
        return task;
    }
    return nullptr;
}


/// @return true if _activeChunk will point to a different chunk when getTask is called.
// This function is normally used by other classes to determine if it is a reasonable time
// to change priority.
bool ChunkTasksQueue::nextTaskDifferentChunkId() {
    std::lock_guard<std::mutex> lock(_mapMx);
    if (_activeChunk == _chunkMap.end()) {
        return true;
    }
    return _activeChunk->second->readyToAdvance();
}


/// This is called when a Task finishes.
void ChunkTasksQueue::taskComplete(wbase::Task::Ptr const& task) {
    std::lock_guard<std::mutex> lock(_mapMx);
    auto iter = _chunkMap.find(task->getChunkId());
    if (iter != _chunkMap.end()) {
        iter->second->taskComplete(task);
    }
}


bool ChunkTasksQueue::setResourceStarved(bool starved) {
    bool ret = _resourceStarved;
    _resourceStarved = starved;
    return ret;
}


int ChunkTasksQueue::getActiveChunkId() {
    std::lock_guard<std::mutex> lock(_mapMx);
    if (_activeChunk == _chunkMap.end()) {
        return -1;
    }
    return _activeChunk->second->getChunkId();
}


wbase::Task::Ptr ChunkTasksQueue::removeTask(wbase::Task::Ptr const& task) {
    // Find the correct chunk
    auto chunkId = task->getChunkId();
    std::lock_guard<std::mutex> lock(_mapMx);
    auto iter = _chunkMap.find(chunkId);
    if (iter == _chunkMap.end()) return nullptr;

    // Erase the task if it is in the chunk
    ChunkTasks::Ptr ct = iter->second;
    auto ret = ct->removeTask(task);
    if (ret != nullptr) {
        --_taskCount; // Need to do this as getTask() wont be called for task.
    }
    return ret;
}


bool ChunkTasksQueue::empty() const {
    std::lock_guard<std::mutex> lock(_mapMx);
    return _empty();
}

}}} // namespace lsst::qserv::wsched
