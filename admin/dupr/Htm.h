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

    Note that if the subdivision level of the target trixels is different
    from that of the source trixels, the transform above can be used to
    derive a catalog of greater or smaller density from an input catalog,
    with relative angular structure roughly preserved.

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
#include <utility>
#include <vector>
#include "boost/scoped_array.hpp"

#include "Hash.h"
#include "Vector.h"


namespace dupr {

/// Maximum HTM subdivision level such that an ID requires <32 bits.
int const HTM_MAX_LEVEL = 13;

/// Compute the HTM ID of the given point.
uint32_t htmId(Vector3d const &v, int level);

/// Return the subdivision level of the given ID or -1 if the ID is invalid.
int htmLevel(uint32_t id);

/// Return the unit 3-vector corresponding to the given right ascension
/// and declination (in degrees).
Vector3d const cartesian(std::pair<double, double> const &radec);

/// Return the right ascension and declination (in degrees) corresponding
/// to the given 3-vector.
std::pair<double, double> const spherical(Vector3d const &v);


/** An HTM triangle.
  */
class Trixel {
public:
    /// Construct the trixel with the given HTM ID.
    explicit Trixel(uint32_t htmId);

    /// Return matrix that converts from cartesian to spherical barycentric coordinates.
    Matrix3d const & getBarycentricTransform() const {
        return _mi;
    }
    /// Return matrix that converts from spherical barycentric to cartesian coordinates.
    Matrix3d const getCartesianTransform() const {
        return _m;
    }

private:
    /// [v1 v2 v3], where column vectors are triangle vertices.
    Matrix3d _m;
    /// inverse of _m
    Matrix3d _mi;
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
    uint32_t mapToNonEmptyTrixel(uint32_t id) const {
        if (getNumRecords(id) != 0) {
            return id;
        }
        return _nonEmpty[mulveyHash(id) % _nonEmpty.size()];
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


/** A spherical coordinate space bounding box.

    This is similar to a bounding box in cartesian space in that
    it is specified by a pair of points; however, a spherical box may
    correspond to the entire unit-sphere, a spherical cap, a lune or
    the traditional rectangle. Additionally, spherical boxes can span
    the 0/360 degree longitude angle discontinuity.
  */
class SphericalBox {
public:
    SphericalBox() : _raMin(0.0), _raMax(360.0), _decMin(-90.0), _decMax(90.0) { }

    SphericalBox(double raMin, double raMax, double decMin, double decMax);

    /// Create a conservative bounding box for the triangle with vertices
    /// stored as column vectors in m.
    SphericalBox(Matrix3d const & m);

    /// Expand the box by the given radius.
    void expand(double radius);

    /// Is the box empty?
    bool isEmpty() const {
        return _decMax < _decMin;
    }

    /// Does the box wrap around the 0/360 right ascension discontinuity?
    bool wraps() const {
        return _raMax < _raMin;
    }

    /// Return the extent in right ascension of this box.
    double getRaExtent() const {
        if (wraps()) {
            return 360.0 - _raMin + _raMax;
        }
        return _raMax - _raMin;
    }

    /// Does this box intersect the given box?
    bool intersects(SphericalBox const & box) const {
        if (box.isEmpty()) {
            return false;
        } else if (box._decMin > _decMax || box._decMax < _decMin) {
            return false;
        } else if (wraps()) {
            if (box.wraps()) {
                return true;
            }
            return box._raMin <= _raMax || box._raMax >= _raMin;
        } else if (box.wraps()) {
            return _raMin <= box._raMax || _raMax >= box._raMin;
        }
        return  _raMin <= box._raMax && _raMax >= box._raMin;
    }

    /// Return a list of IDs for HTM trixels potentially overlapping this box.
    /// IDs of all overlapping HTM trixels are returned, but IDs for nearby
    /// trixels that do not actually overlap may also be included.
    std::vector<uint32_t> const htmIds(int level) const;

private:
    void findIds(uint32_t id,
                 int level,
                 Matrix3d const & m,
                 std::vector<uint32_t> & ids) const;

    double _raMin;
    double _raMax;
    double _decMin;
    double _decMax;
};


/** A chunk location for a position.
  */
struct ChunkLocation {
    enum Overlap {
        CHUNK = 0,    // not an overlap location
        SELF_OVERLAP, // self-overlap location (also a full-overlap location)
        FULL_OVERLAP, // full-overlap location
    };
    int32_t chunkId;
    int32_t subChunkId;
    Overlap overlap;
};

/** A class that assigns points to partitions using a simple scheme that
    breaks the unit sphere into fixed height declination stripes. These are
    in turn broken up into fixed width right ascension chunks (each stripe
    has a variable number of chunks to account for distortion at the poles).
    Chunks are in turn broken up into fixed height sub-stripes, and each
    sub-stripe is then divided into fixed width sub-chunks. As before,
    the number of sub-chunks per sub-stripe is variable to account for
    polar distortion. This class also provides methods for retrieving
    bounding boxes of chunks and sub-chunks, as well as for assigning
    chunks to (qserv worker) nodes.
  */
class Chunker {
public:
    Chunker(double overlap, int32_t numStripes, int32_t numSubStripesPerStripe);
    ~Chunker();

    double getOverlap() const { return _overlap; }

    /// Return a bounding box for the given chunk.
    SphericalBox const getChunkBounds(int32_t chunkId) const;

    /// Return a bounding box for the given sub-chunk.
    SphericalBox const getSubChunkBounds(int32_t chunkId, int32_t subChunkId) const;

    /// Append locations of the given position to the locations vector.
    /// If chunkId is negative, all locations will be appended. Otherwise,
    /// only those with the given chunk ID will be appended.
    void locate(std::pair<double, double> const & position,
                int32_t chunkId,
                std::vector<ChunkLocation> & locations) const;

    /// Return IDs of all chunks overlapping the given region and belonging
    /// to the given node. The target node is specifed as an integer in
    /// range [0, numNodes). If hash is true, then chunk C is assigned to
    /// the node given by hash(C) modulo numNodes. Otherwise, chunks are
    /// assigned to nodes in round-robin fashion. The region argument has no
    /// effect on which server a chunk C is assigned to.
    std::vector<int32_t> const getChunksFor(SphericalBox const & region,
                                            uint32_t node,
                                            uint32_t numNodes,
                                            bool hashChunks) const;

private:
    // Disable copy construction and assignment.
    Chunker(Chunker const &);
    Chunker & operator=(Chunker const &);

    // conversion between IDs and indexes
    int32_t getStripe(int32_t chunkId) const {
        return chunkId / (2*_numStripes);
    }
    int32_t getSubStripe(int32_t subChunkId, int32_t stripe) const {
        return stripe*_numSubStripesPerStripe + subChunkId/_maxSubChunksPerChunk;
    }
    int32_t getChunk(int32_t chunkId, int32_t stripe) const {
        return chunkId - stripe*2*_numStripes;
    }
    int32_t getSubChunk(int32_t subChunkId, int32_t stripe,
                        int32_t subStripe, int32_t chunk) const {
        return subChunkId -
               (subStripe - stripe*_numSubStripesPerStripe)*_maxSubChunksPerChunk +
               chunk*_numSubChunksPerChunk[subStripe];
    }
    int32_t getChunkId(int32_t stripe, int32_t chunk) const {
        return stripe*2*_numStripes + chunk;
    }
    int32_t getSubChunkId(int32_t stripe, int32_t subStripe,
                          int32_t chunk, int32_t subChunk) const {
        return (subStripe - stripe*_numSubStripesPerStripe)*_maxSubChunksPerChunk +
               (subChunk - chunk*_numSubChunksPerChunk[subStripe]);
    }

    void upDownOverlap(double ra,
                       int32_t chunkId,
                       ChunkLocation::Overlap overlap,
                       int32_t stripe,
                       int32_t subStripe,
                       std::vector<ChunkLocation> & locations) const;

    double _overlap;
    double _subStripeHeight;
    int32_t _numStripes;
    int32_t _numSubStripesPerStripe;
    /// Maximum number of sub-chunks per chunk across all sub-stripes.
    int32_t _maxSubChunksPerChunk;
    /// Number of chunks per stripe, indexed by stripe.
    boost::scoped_array<int32_t> _numChunksPerStripe;
    /// Number of sub-chunks per chunk, indexed by sub-stripe.
    boost::scoped_array<int32_t> _numSubChunksPerChunk;
    /// Sub-chunk width (in RA) for each sub-stripe.
    boost::scoped_array<double> _subChunkWidth;
    /// For each sub-stripe, provides the maximum half-width (in RA)
    /// of a circle with radius _overlap and center inside the sub-stripe.
    /// Guaranteed to be smaller than the sub-chunk width.
    boost::scoped_array<double> _alpha;
};

} // namespace dupr

#endif
