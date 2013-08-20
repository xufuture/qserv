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
#ifndef LSST_QSERV_WORKER_CHUNKSTATE_H
#define LSST_QSERV_WORKER_CHUNKSTATE_H
 /**
  *
  * @brief ChunkState is a way to track which chunks are being scanned
  * and which are cached.
  *
  * @author Daniel L. Wang, SLAC
  */

#include <algorithm>
#include <deque>
#include <ostream>
#include <set>

namespace lsst {
namespace qserv {
namespace worker {
class ChunkState {
public:
    typedef std::set<int> IntSet;
    typedef std::deque<int> IntDeque;

    explicit ChunkState(int cacheMax=2) 
        : _cacheMax(cacheMax), _last(-1) {
    }

    void setMax(int cacheMax) { _cacheMax = cacheMax; }

    void addScan(int chunkId) {
        _scan.insert(chunkId);
        _last = chunkId;
        _evictOldElements();        
    }

    void markComplete(int chunkId) {
        if(std::find(_cached.begin(), _cached.end(), chunkId) 
           == _cached.end()) {
            _cached.push_back(chunkId);
        }
        if(std::find(_scan.begin(), _scan.end(), chunkId) != _scan.end()) {
            _scan.erase(chunkId);
        }
        _evictOldElements();
    }
    int isCached(int chunkId) const {
        IntDeque::const_iterator found;
        found = std::find(_cached.begin(), _cached.end(), chunkId);
        return found != _cached.end();
    }
    int isScan(int chunkId) const {
        IntSet::const_iterator found;
        found = std::find(_scan.begin(), _scan.end(), chunkId);
        return found != _scan.end();
    }
    bool empty() const {
        return _scan.empty() && _cached.empty();
    }

    bool hasScan() const {
        return !_scan.empty();
    }

    int lastScan() const {
        return _last;
    }

    friend std::ostream& operator<<(std::ostream& os, ChunkState const& cs);

private:
    inline void _evictOldElements() {
        if(_cached.size() > _cacheMax) {
            _cached.pop_front();
        }
    }
    int _cacheMax;
    IntDeque _cached;
    IntSet _scan;
    int _last;
};


}}} // lsst::qserv::worker
#endif // LSST_QSERV_WORKER_CHUNKSTATE_H

