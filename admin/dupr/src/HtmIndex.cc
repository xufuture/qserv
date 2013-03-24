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

#include "HtmIndex.h"

#include <algorithm>
#include <iomanip>
#include <stdexcept>

#include "boost/scoped_array.hpp"

#include "Constants.h"
#include "FileUtils.h"
#include "Geometry.h"
#include "Hash.h"

using std::ostream;
using std::runtime_error;
using std::setw;
using std::sort;
using std::vector;

namespace fs = boost::filesystem;


namespace lsst { namespace qserv { namespace admin { namespace dupr {

HtmIndex::HtmIndex(int level) :
    _numRecords(0),
    _recordSize(0),
    _map(),
    _keys(),
    _level(level)
{
    if (level < 0 || level > HTM_MAX_LEVEL) {
        throw runtime_error("Invalid HTM subdivision level.");
    }
}

HtmIndex::HtmIndex(fs::path const & path) :
    _numRecords(0),
    _recordSize(0),
    _map(),
    _keys(),
    _level(-1)
{
    _read(path);
}

HtmIndex::HtmIndex(vector<fs::path> const & paths) :
    _numRecords(0),
    _recordSize(0),
    _map(),
    _keys(),
    _level(-1)
{
    typedef vector<fs::path>::const_iterator Iter;
    if (paths.empty()) {
        throw runtime_error("Empty HTM index file list.");
    }
    for (Iter i = paths.begin(), e = paths.end(); i != e; ++i) {
        _read(*i);
    }
}

HtmIndex::HtmIndex(HtmIndex const & idx) :
    _numRecords(idx._numRecords),
    _recordSize(idx._recordSize),
    _map(idx._map),
    _keys(idx._keys),
    _level(idx._level)
{ }

HtmIndex::~HtmIndex() { }

HtmIndex::Triangle const & HtmIndex::mapToNonEmpty(uint32_t id) const {
    if (_map.empty()) {
        throw runtime_error("HTM index is empty.");
    }
    Map::const_iterator i = _map.find(id);
    if (i == _map.end()) {
        if (_keys.empty()) {
            // Build sorted list of non-empty HTM triangle IDs.
            _keys.reserve(_map.size());
            for (Map::const_iterator i = _map.begin(), e = _map.end(); i != e; ++i) {
                _keys.push_back(i->first);
            }
            sort(_keys.begin(), _keys.end());
        }
        i = _map.find(_keys[mulveyHash(id) % _keys.size()]);
    }
    return i->second;
}

void HtmIndex::write(fs::path const & path, bool truncate) const {
    size_t const numBytes = _map.size()*(4 + 8 + 8);
    boost::scoped_array<uint8_t> buf(new uint8_t[numBytes]);
    uint8_t * b = buf.get();
    for (Map::const_iterator i = _map.begin(), e = _map.end(); i != e; ++i) {
        Triangle const & t = i->second;
        b = encode(b, t.id);
        b = encode(b, t.numRecords);
        b = encode(b, t.recordSize);
    }
    OutputFile f(path, truncate);
    f.append(buf.get(), numBytes);
}

void HtmIndex::write(ostream & os) const {
    typedef vector<Triangle>::const_iterator TriangleIter;
    // Extract non-empty triangles and sort them by HTM ID.
    vector<Triangle> tris;
    tris.reserve(_map.size());
    for (Map::const_iterator i = _map.begin(), e = _map.end(); i != e; ++i) {
        tris.push_back(i->second);
    }
    sort(tris.begin(), tris.end());
    // Pretty-print the index in JSON format.
    os << "{\n"
          "\"nrec\":      " << _numRecords << ",\n"
          "\"recsz\":     " << _recordSize << ",\n"
          "\"triangles\": [\n";
    for (TriangleIter b = tris.begin(), e = tris.end(), i = b; i != e; ++i) {
        if (i != b) {
            os << ",\n";
        }
        os << "\t{\"id\":"   << setw(10) << i->id
           << ", \"nrec\":"  << setw(8)  << i->numRecords
           << ", \"recsz\":" << setw(10) << i->recordSize
           << "}";
    }
    os << "\n]\n}";
}

HtmIndex::Triangle const & HtmIndex::merge(HtmIndex::Triangle const & tri) {
    if (htmLevel(tri.id) != _level) {
        throw runtime_error("HTM ID is invalid or has an inconsistent "
                            "subdivision level.");
    }
    if (tri.numRecords == 0 || tri.recordSize == 0) {
        throw runtime_error("Updating an HTM index with empty triangles is "
                            "not allowed.");
    }
    Triangle * t = &_map[tri.id];
    if (t->id != tri.id) {
        _keys.clear();
        t->id = tri.id;
    }
    t->numRecords += tri.numRecords;
    _numRecords   += tri.numRecords;
    t->recordSize += tri.recordSize;
    _recordSize   += tri.recordSize;
    return *t;
}

void HtmIndex::merge(HtmIndex const & idx) {
    if (this == &idx) {
        return;
    }
    if (idx._level != _level) {
        throw runtime_error("HTM index subdivision levels do not match.");
    }
    for (Map::const_iterator i = idx._map.begin(), e = idx._map.end();
         i != e; ++i) {
        Triangle * t = &_map[i->second.id];
        if (t->id != i->second.id) {
            _keys.clear();
            t->id = i->second.id;
        }
        t->numRecords += i->second.numRecords;
        _numRecords   += i->second.numRecords;
        t->recordSize += i->second.recordSize;
        _recordSize   += i->second.recordSize;
    }
}

void HtmIndex::clear() {
    _numRecords = 0;
    _recordSize = 0;
    _map.clear();
    _keys.clear();
}

void HtmIndex::swap(HtmIndex & idx) {
    using std::swap;
    if (this != &idx) {
        swap(_numRecords, idx._numRecords);
        swap(_recordSize, idx._recordSize);
        swap(_map, idx._map);
        swap(_keys, idx._keys);
        swap(_level, idx._level);
    }
}

HtmIndex::Triangle const HtmIndex::EMPTY;

void HtmIndex::_read(fs::path const & path) {
    InputFile f(path);
    if (f.size() == 0 || (f.size()) % (4 + 8 + 8) != 0) {
        throw runtime_error("Invalid HTM index file.");
    }
    boost::scoped_array<uint8_t> data(new uint8_t[f.size()]);
    f.read(data.get(), 0, f.size());
    uint8_t const * b = data.get();
    off_t const numTriangles = f.size()/(4 + 8 + 8);
    for (off_t i = 0; i < numTriangles; ++i, b += 4 + 8 + 8) {
        uint32_t id = decode<uint32_t>(b);
        uint64_t numRecords = decode<uint64_t>(b + 4);
        uint64_t recordSize = decode<uint64_t>(b + 12);
        int level = htmLevel(id);
        if (level < 0 || level > HTM_MAX_LEVEL) {
            throw runtime_error("Invalid HTM index file.");
        }
        if (_level < 0) {
            _level = level;
        } else if (level != _level) {
            throw runtime_error("HTM index subdivision levels do not match.");
        }
        if (numRecords == 0 || recordSize ==0) {
            throw runtime_error("HTM index file contains an empty triangle.");
        }
        Triangle * t = &_map[id];
        t->id = id;
        t->numRecords += numRecords;
        _numRecords   += numRecords;
        t->recordSize += recordSize;
        _recordSize   += recordSize;
    }
}

}}}} // namespace lsst::qserv::admin::dupr
