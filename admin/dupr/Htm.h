/** Machinery for spherical geometry and Hierarchical Triangular Mesh indexing.

    Simple functions for indexing points and converting between spherical
    and cartesian coordinate systems are provided. The Trixel class,
    corresponding to a triangle in a Hierarchical Triangular Mesh, exists
    to allow conversion between cartesian 3-vectors and spherical barycentric
    coordinates.

    The spherical barycentric coordinates b1, b2 and b3 of a 3-vector V,
    given linearly independent triangle vertices V1, V2 and V3,
    are defined as the solution to:

    b1 * V1 + b2 * V2 + b3 * V3 = V

    If we let the column vector B = transpose([b1 b2 b3]) and M be the
    3x3 matrix with column vectors V1, V2 and V3, we can write the above
    more simply as:

    M * B = V

    Denoting the inverse of M by M^-1, this yields:

    B = M^-1 * V

    What are such coordinates used for?

    Well, at a very high level, the duplicator works by building a map of
    non-empty HTM triangles. It converts the coordinates of each point to
    spherical barycentric form. Then, to populate an empty triangle _e, the
    duplicator chooses a non-empty triangle _t and copies all its points.
    For a point V in _t, the position of the copy is set to

    M_e * (M_t^-1 * V) = (M_e * M_t^-1) * V

    In other words, V is transformed by the matrix that maps the vertices of
    _t to the vertices of _e. Since the area and proportions of different HTM
    triangles don't vary all that much, one can think of (M_e * M_t^-1) as
    something fairly close to a rotation. The fact that the transform isn't
    quite length preserving doesn't matter; after all, cartesian coordinates
    V and k*V (k > 0) map to the same spherical coordinates. Unlike an
    approach that shifts around copies of an input data set in spherical
    coordinate space, there are no serious distortion issues to worry about
    near the poles.

    The PopulationMap class tracks which trixels in an input data set
    contain records, and provides a surjection from the set of all HTM IDs
    (at a given level L) to the set of HTM IDs for trixels containing at
    least one record. Note that handling input data-sets obtained from
    multiple disjoint observation regions (or regions not well approximated
    by a box in spherical coordinate space) is straightforward.

    PopulationMap also stores an index into an HTM-id sorted CSV file
    containing the intput data. This means the duplicator can read the
    CSV data for a non-empty trixel as a single contiguous block.
  */
#ifndef HTM_H
#define HTM_H

#include <stdint.h>
#include <vector>
#include "boost/scoped_array.hpp"
#include "Eigen/Dense"


namespace dupr {

/// Maximum HTM subdivision level such that an ID requires <32 bits.
int const HTM_MAX_LEVEL = 13;

/// Compute the HTM ID of the given point.
uint32_t htmId(Eigen::Vector3d const &v, int level);

/// Return the subdivision level of the given ID or -1 if the ID is invalid.
int htmLevel(uint32_t id);

/// Return the unit 3-vector corresponding to the given right ascension
/// and declination (in degrees).
Eigen::Vector3d const cartesian(Eigen::Vector2d const &radec);

/// Return the right ascension and declination (in degrees) corresponding
/// to the given 3-vector.
Eigen::Vector2d const spherical(Eigen::Vector3d const &v);


/** An HTM triangle.
  */
class Trixel {
public:
    /// Construct the trixel with the given HTM ID.
    Trixel(uint32_t htmId);

    /// Convert from cartesian to spherical barycentric coordinates.
    Eigen::Vector3d const barycentric(Eigen::Vector3d const &v) const {
        return _mi * v;
    }
    /// Convert from spherical barycentric to cartesian coordinates.
    Eigen::Vector3d const cartesian(Eigen::Vector3d const &b) const {
        return _m * b;
    }

private:
    /// [v1 v2 v3], where column vectors are triangle vertices.
    Eigen::Matrix3d _m;
    /// inverse of _m
    Eigen::Matrix3d _mi;
};


/** A population map of the sky.

    A population map can be used to obtain the number of records in a trixel,
    the offset of the first CSV record in a trixel, the size of all CSV records
    in a trixel, or to map any trixel to a non-empty trixel.

    Instances are in one of 2 states: queryable or in-construction. An
    instance read from a file starts in the queryable state. Otherwise, it
    begins life in-construction and can be built up via add().
    Calling makeQueryable() transitions the map to the queryable state.
  */
class PopulationMap {
public:
    // Create an empty population map. The new map will be in-construction.
    PopulationMap(int level);

    // Read a population map from a file. The new map will be queryable.
    PopulationMap(std::string const & path);

    ~PopulationMap();

    /// Return the HTM subdivision level of the map.
    int getLevel() const {
        return _level;
    }

    /// Return the total number of trixels in the map.
    uint32_t getNumTrixels() const {
        return _numTrix;
    }

    // -- OK to call while map is in-construction ----

    /// Add numRecords records occupying size bytes to the map.
    /// Safe to call from multiple threads.
    void add(uint32_t id, uint64_t numRecords, uint64_t size) {
        __sync_fetch_and_add(&_count[id - _numTrix + 1], numRecords);
        __sync_fetch_and_add(&_offset[id - _numTrix + 1], size);
    }

    /// Transition to queryable state.
    void makeQueryable();

    // -- OK to call once map is queryable ----

    /// Return the total number of records in the map.
    uint64_t getNumRecords() const {
        return _count[_numTrix];
    }
    /// Return the number of records in a trixel.
    uint32_t getNumRecords(uint32_t id) const {
        return static_cast<uint32_t>(
            _count[id - _numTrix + 1] - _count[id - _numTrix]);
    }
    /// Return the number of records with HTM ID less than id.
    uint64_t getNumRecordsBelow(uint32_t id) const {
        return _count[id - _numTrix];
    }
    /// Return the total size of all CSV records in the map.
    uint64_t getSize() const {
        return _offset[_numTrix];
    }
    /// Return the total size of all CSV records in a trixel.
    uint32_t getSize(uint32_t id) const {
        return static_cast<uint32_t>(
            _offset[id - _numTrix + 1] - _offset[id - _numTrix]);
    }
    /// Return the offset of the first CSV record in a trixel.
    uint64_t getOffset(uint32_t id) const {
        return _offset[id - _numTrix];
    }
    /// Return the number of non-empty trixels.
    size_t getNumNonEmpty() const {
        return _nonEmpty.size();
    }
    /// Map a trixel to a non-empty trixel.
    uint32_t mapTrixel(uint32_t id) const {
        if (getNumRecords(id) != 0) {
            return id;
        }
        return _nonEmpty[id % _nonEmpty.size()];
    }

    /// Write the population map to a file.
    void write(std::string const & path) const;

private:
    // Disable copy construction and assignment.
    PopulationMap(PopulationMap const &);
    PopulationMap & operator=(PopulationMap const &);

    uint32_t _numTrix;
    int _level;
    bool _queryable;

    boost::scoped_array<uint64_t> _count;
    boost::scoped_array<uint64_t> _offset;
    std::vector<uint32_t> _nonEmpty;
};

} // namespace dupr

#endif
