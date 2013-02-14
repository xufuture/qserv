#include "HtmIndex.h"

#include <algorithm>
#include <iomanip>
#include <stdexcept>

#include "boost/static_assert.hpp"

#include "FileUtils.h"
#include "Geometry.h"
#include "Hash.h"

using std::cout;
using std::endl;
using std::ostream;
using std::pair;
using std::runtime_error;
using std::setw;
using std::sort;
using std::string;
using std::swap;
using std::vector;


namespace dupr {

namespace {

    // write integers to a byte buffer
    uint8_t * encode(uint32_t x, uint8_t * buf) {
        buf[0] = static_cast<uint8_t>( x        & 0xff);
        buf[1] = static_cast<uint8_t>((x >>  8) & 0xff);
        buf[2] = static_cast<uint8_t>((x >> 16) & 0xff);
        buf[3] = static_cast<uint8_t>((x >> 24) & 0xff);
        return buf + 4;
    }
    uint8_t * encode(uint64_t x, uint8_t * buf) {
        buf[0] = static_cast<uint8_t>( x        & 0xff);
        buf[1] = static_cast<uint8_t>((x >>  8) & 0xff);
        buf[2] = static_cast<uint8_t>((x >> 16) & 0xff);
        buf[3] = static_cast<uint8_t>((x >> 24) & 0xff);
        buf[4] = static_cast<uint8_t>((x >> 32) & 0xff);
        buf[5] = static_cast<uint8_t>((x >> 40) & 0xff);
        buf[6] = static_cast<uint8_t>((x >> 48) & 0xff);
        buf[7] = static_cast<uint8_t>((x >> 56) & 0xff);
        return buf + 8;
    }

    // read integers from a byte buffer
    template <typename T> T decode(uint8_t const * buf) {
        BOOST_STATIC_ASSERT(sizeof(T) == 0);
    }
    template <> uint32_t decode<uint32_t>(uint8_t const * buf) {
        return  static_cast<uint32_t>(buf[0]) |
               (static_cast<uint32_t>(buf[1]) << 8) |
               (static_cast<uint32_t>(buf[2]) << 16) |
               (static_cast<uint32_t>(buf[3]) << 24);
    }
    template <> uint64_t decode<uint64_t>(uint8_t const * buf) {
        return  static_cast<uint64_t>(buf[0]) |
               (static_cast<uint64_t>(buf[1]) << 8) |
               (static_cast<uint64_t>(buf[2]) << 16) |
               (static_cast<uint64_t>(buf[3]) << 24) |
               (static_cast<uint64_t>(buf[4]) << 32) |
               (static_cast<uint64_t>(buf[5]) << 40) |
               (static_cast<uint64_t>(buf[6]) << 48) |
               (static_cast<uint64_t>(buf[7]) << 56);
    }

} // unnamed namespace


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

HtmIndex::HtmIndex(string const & path) :
    _numRecords(0),
    _recordSize(0),
    _map(),
    _keys(),
    _level(-1)
{
    read(path);
}

HtmIndex::HtmIndex(vector<string> const & paths) :
    _numRecords(0),
    _recordSize(0),
    _map(),
    _keys(),
    _level(-1)
{
    typedef vector<string>::const_iterator Iter;
    if (paths.empty()) {
        throw runtime_error("Empty HTM index file list.");
    }
    for (Iter i = paths.begin(), e = paths.end(); i != e; ++i) {
        read(*i);
    }
}

HtmIndex::~HtmIndex() { }

HtmIndex::Triangle const & HtmIndex::mapToNonEmpty(uint32_t id) const {
    if (_map.empty()) {
        throw runtime_error("HTM index is empty.");
    }
    Map::const_iterator i = _map.find(id);
    if (i == _map.end()) {
        if (_keys.empty()) {
            // build sorted list of non-empty HTM triangle IDs on demand
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

HtmIndex::Triangle const & HtmIndex::update(HtmIndex::Triangle const & tri) {
    if (htmLevel(tri.id) != _level) {
        throw runtime_error("HTM ID is invalid or does not match the index "
                            "subdivision level.");
    }
    if (tri.numRecords == 0 || tri.recordSize == 0) {
        throw runtime_error("Updating an HTM index with empty triangles is "
                            "not allowed.");
    }
    Triangle * t = &_map[tri.id];
    if (t->id != tri.id) {
        // t was just added - clear HTM ID cache
        _keys.clear();
        t->id = tri.id;
    }
    t->numRecords += tri.numRecords;
    t->recordSize += tri.recordSize;
    return *t;
}

void HtmIndex::write(string const & path) const {
    size_t const numBytes = _map.size()*(4 + 8 + 8) + 1;
    boost::scoped_array<uint8_t> buf(new uint8_t[numBytes]);
    uint8_t * b = buf.get();
    b[0] = static_cast<uint8_t>(_level);
    ++b;
    for (Map::const_iterator i = _map.begin(), e = _map.end(); i != e; ++i) {
        Triangle const & t = i->second;
        b = encode(t.id, b);
        b = encode(t.numRecords, b);
        b = encode(t.recordSize, b);
    }
    OutputFile f(path);
    f.append(buf.get(), numBytes);
}

ostream & HtmIndex::operator<<(ostream & os) const {
    vector<Triangle> tris;
    tris.reserve(_map.size());
    for (Map::const_iterator i = _map.begin(), e = _map.end(); i != e; ++i) {
        tris.push_back(i->second);
    }
    sort(tris.begin(), tris.end());
    for (vector<Triangle>::const_iterator i = tris.begin(), e = tris.end();
         i != e; ++i) {
        os << "Triangle "
           << setw(9)  << i->id         << " : "
           << setw(8)  << i->numRecords << " records, "
           << setw(10) << i->recordSize << "bytes\n";
    }
    return os;
}

HtmIndex::Triangle const HtmIndex::EMPTY;

void HtmIndex::read(string const & path) {
    InputFile f(path);
    if (f.size() < 1 || (f.size() - 1) % (4 + 8 + 8) != 0) {
        throw runtime_error("Invalid HTM index file.");
    }
    boost::scoped_array<uint8_t> data(new uint8_t[f.size()]);
    f.read(data.get(), 0, f.size());
    uint8_t const * b = data.get();
    int const level = static_cast<int>(*b);
    if (_level < 0) {
        if (level < 0 || level > HTM_MAX_LEVEL) {
            throw runtime_error("Invalid HTM index file.");
        }
        _level = level;
    } else if (_level != level) {
        throw runtime_error("Cannot merge HTM index files with "
                            "inconsistent subdivision levels.");
    }
    ++b;
    off_t const numTriangles = (f.size() - 1)/(4 + 8 + 8);
    for (off_t i = 0; i < numTriangles; ++i, b += 20) {
        uint32_t id = decode<uint32_t>(b);
        uint64_t numRecords = decode<uint64_t>(b + 4);
        uint64_t recordSize = decode<uint64_t>(b + 12);
        if (htmLevel(id) != level) {
            throw runtime_error("HTM ID in index file is invalid or does "
                                "not match the index subdivision level.");
        }
        if (numRecords == 0 || recordSize ==0) {
            throw runtime_error("HTM index file contains an empty triangle.");
        }
        Triangle * t = &_map[id];
        t->id = id;
        t->numRecords += numRecords;
        t->recordSize += recordSize;
    }
}

} // namespace dupr

