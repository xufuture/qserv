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
#ifndef LSST_QSERV_WSCHED_SLOWTABLEHEAP_H
#define LSST_QSERV_WSCHED_SLOWTABLEHEAP_H

// System headers
#include <algorithm>
#include <vector>

// Qserv headers
#include "wbase/Task.h"

namespace lsst {
namespace qserv {
namespace wsched {

/// Class that keeps the slowest tables at the front of the heap.
class SlowTableHeap {
public:
    // Using a greater than comparison function results in a minimum value heap.
    static bool compareFunc(wbase::Task::Ptr const& x, wbase::Task::Ptr const& y) {
        if(!x || !y) { return false; }
        // compare scanInfo (slower scans first)
        int siComp = x->getScanInfo().compareTables(y->getScanInfo());
        return siComp < 0;
    };
    void push(wbase::Task::Ptr const& task);
    wbase::Task::Ptr pop();
    wbase::Task::Ptr top() {
        if (_tasks.empty()) return nullptr;
        return _tasks.front();
    }
    bool empty() const { return _tasks.empty(); }
    size_t size() const { return _tasks.size(); }
    void heapify() {
        std::make_heap(_tasks.begin(), _tasks.end(), compareFunc);
    }

    std::vector<wbase::Task::Ptr> _tasks;
};

}}} // namespace lsst::qserv::wsched

#endif // LSST_QSERV_WSCHED_SLOWTABLEHEAP_H
