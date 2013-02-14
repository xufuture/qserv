/** An HTM index over a set of records having a position on the sky.
  */
#ifndef HTM_INDEX_H
#define HTM_INDEX_H

#include <stdint.h>
#include <ostream>
#include <string>
#include <vector>

#include "boost/unordered_map.hpp"


namespace dupr {

/** An HtmIndex tracks which HTM triangles at a given subdivision level
    contain records of an input data set, as well as the number and total
    size of records in each triangle. It also provides a mapping from the
    set of all HTM IDs to the set of HTM IDs for triangles containing at
    least one record.
  */
class HtmIndex {
public:
    /// An index entry.
    struct Triangle {
        uint32_t id;         ///< HTM triangle ID.
        uint64_t numRecords; ///< Number of records in the triangle.
        uint64_t recordSize; ///< Size in bytes of triangle records.

        Triangle() : id(0), numRecords(0), recordSize(0) { }

        Triangle(uint32_t i, uint64_t n, uint64_t s) :
            id(i), numRecords(n), recordSize(s) { }

        bool operator<(Triangle const & t) const { return id < t.id; }
    };

    /// Create an empty HTM index at the given subdivision level.
    HtmIndex(int level);
    /// Read an HTM index from a file.
    HtmIndex(std::string const & path);
    /// Read and merge a list of HTM index files.
    HtmIndex(std::vector<std::string> const & paths);

    ~HtmIndex();

    /// Return the total number of records tracked by the index.
    uint64_t getNumRecords() const { return _numRecords; }
    /// Return the size in bytes of all records tracked by the index.
    uint64_t getRecordSize() const { return _recordSize; }
    /// Return record count and size for the triangle with the given ID.
    Triangle const & operator()(uint32_t id) const {
        Map::const_iterator i = _map.find(id);
        if (i == _map.end()) {
            return EMPTY;
        }
        return i->second;
    }
    /// Return the number of non-empty triangles in the index.
    size_t size() const { return _map.size(); }
    bool empty() const { return size() == 0; }

    /// Map the given triangle to a non-empty triangle in a deterministic way.
    Triangle const & mapToNonEmpty(uint32_t id) const;

    /// Add or merge the given triangle with this index, returning a reference
    /// to the newly added or updated triangle.
    Triangle const & update(Triangle const & tri);

    /// Write the index to a file in an implementation-defined binary format.
    void write(std::string const & path) const;
    /// Print the index to a stream in human readable format.
    std::ostream & operator<<(std::ostream & os) const;

private:
    // Disable copy construction and assignment.
    HtmIndex(HtmIndex const &);
    HtmIndex & operator=(HtmIndex const &);

    static Triangle const EMPTY;

    void read(std::string const & path);
    void computeKeys() const;

    typedef boost::unordered_map<uint32_t, Triangle> Map;

    uint64_t _numRecords;
    uint64_t _recordSize;
    Map _map;
    mutable std::vector<uint32_t> _keys;
    int _level;
};

} // namespace dupr

#endif // HTM_INDEX_H_
