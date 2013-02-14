/** Machinery for spherical geometry and Hierarchical Triangular Mesh indexing.

    Simple functions for indexing points and converting between spherical and
    cartesian coordinate systems are provided. The SphericalTriangle and
    SphericalBox classes represent spherical regions, and tThe Chunker class
    is responsible for finding the storage locations of points according to
    the Qserv partitioning strategy.
  */
#ifndef GEOMETRY_H
#define GEOMETRY_H

#include <stdint.h>
#include <utility>
#include <vector>
#include "boost/scoped_array.hpp"

#include "Vector.h"


namespace dupr {

double const DEG_PER_RAD = 57.2957795130823208767981548141;   /// 180/π
double const RAD_PER_DEG = 0.0174532925199432957692369076849; /// π/180
double const EPSILON_DEG = 0.001 / 3600;                      /// 1 mas

/// Maximum HTM subdivision level such that an ID requires less than 32 bits.
int const HTM_MAX_LEVEL = 13;

/// Compute the HTM ID of the given point.
uint32_t htmId(Vector3d const &v, int level);

/// Return the subdivision level of the given ID or -1 if the ID is invalid.
int htmLevel(uint32_t id);

/// Return the unit 3-vector corresponding to the given right ascension
/// and declination (in degrees).
Vector3d const cartesian(std::pair<double, double> const &radec);

inline Vector3d const cartesian(double ra, double dec) {
    return cartesian(std::pair<double, double>(ra, dec));
}

/// Return the right ascension and declination (in degrees) corresponding
/// to the given 3-vector.
std::pair<double, double> const spherical(Vector3d const &v);

inline std::pair<double, double> const spherical(double x, double y, double z) {
    return spherical(Vector3d(x,y,z));
}


class SphericalBox;

/** A triangle on the surface of the unit sphere with great-circle edges.

    The main purpose of this class is to allow conversion between cartesian
    3-vectors and spherical barycentric coordinates.

    The spherical barycentric coordinates b1, b2 and b3 of a 3-vector V,
    given linearly independent triangle vertices V1, V2 and V3,
    are defined as the solution to:

    b1*V1 + b2*V2 + b3*V3 = V

    If we let the column vector B = transpose([b1 b2 b3]) and M be the
    3x3 matrix with column vectors V1, V2 and V3, we can write the above
    more simply as:

    M * B = V

    or

    B = M⁻¹ * V

    What are such coordinates used for?

    Well, at a very high level, the duplicator works by building a map of
    non-empty HTM triangles. It converts the coordinates of each point to
    spherical barycentric form. Then, to populate an empty triangle u, the
    duplicator chooses a non-empty triangle v and copies all its points.
    For a point V in v, the position of the copy is set to

    Mᵤ * (Mᵥ⁻¹ * V) = (Mᵤ * Mᵥ⁻¹) * V

    In other words, V is transformed by the matrix that maps the vertices of
    v to the vertices of u. Since the area and proportions of different HTM
    triangles don't vary all that much, one can think of (Mᵤ * Mᵥ⁻¹) as
    something fairly close to a rotation. The fact that the transform isn't
    quite length preserving doesn't matter; after all, cartesian coordinates
    V and k*V (k > 0) map to the same spherical coordinates. Unlike an
    approach that shifts around copies of an input data set in spherical
    coordinate space, there are no serious distortion issues to worry about
    near the poles.

    Note that if the subdivision level of the target triangles is different
    from that of the source trixels, the transform above can be used to
    derive a catalog of greater or smaller density from an input catalog,
    with relative angular structure roughly preserved.
  */
class SphericalTriangle {
public:
    /// Construct the HTM triangle with the given HTM ID.
    explicit SphericalTriangle(uint32_t htmId);

    SphericalTriangle(Vector3d const & v0, Vector3d const & v1, Vector3d const & v2);

    /// Get the i-th vertex (i = 0,1,2). No bounds checking is performed.
    Vector3d const & vertex(int i) const { return _m.col(i); }

    /// Get the matrix that converts from cartesian to
    /// spherical barycentric coordinates.
    Matrix3d const & getBarycentricTransform() const {
        return _mi;
    }
    /// Get the matrix that converts from spherical barycentric
    /// to cartesian coordinates.
    Matrix3d const getCartesianTransform() const {
        return _m;
    }

    /// Compute the area of the triangle in steradians.
    double area() const;

    /// Compute the area of the surface obtained by intersecting this triangle
    /// with a spherical box. The routine is not fully general - for simplicity
    /// of implementation, spherical boxes with RA extent strictly between 180
    /// and 360 degrees are not supported.
    double intersectionArea(SphericalBox const & box) const;

private:
    // [V0 V1 V2], where column vectors V0, V1, V2 are the triangle
    // vertices (unit vectors).
    Matrix3d _m;
    // Inverse of _m, corresponding to
    // transpose([V1 x V2, V2 x V0, V0 x V1])/det(_m).
    Matrix3d _mi;
};


/** A spherical coordinate space bounding box.

    This is similar to a bounding box in cartesian space in that it is
    specified by a pair of points; however, a spherical box may correspond
    to the entire unit-sphere, a spherical cap, a lune or the traditional
    rectangle. Additionally, spherical boxes can span the 0/360 degree
    longitude angle discontinuity.
  */
class SphericalBox {
public:
    SphericalBox() : _raMin(0.0), _raMax(360.0), _decMin(-90.0), _decMax(90.0) { }

    SphericalBox(double raMin, double raMax, double decMin, double decMax);

    /// Create a conservative bounding box for a spherical triangle.
    SphericalBox(Vector3d const & v0, Vector3d const & v1, Vector3d const & v2);

    /// Expand the box by the given radius.
    void expand(double radius);

    bool isEmpty() const {
        return _decMax < _decMin;
    }
    bool isFull() const {
        return _decMin == -90.0 && _decMax == 90.0 &&
               _raMin == 0.0 && _raMax == 360.0;
    }

    /// Does the box wrap around the 0/360 right ascension discontinuity?
    bool wraps() const {
        return _raMax < _raMin;
    }

    double getRaMin() const { return _raMin; }
    double getRaMax() const { return _raMax; }
    double getDecMin() const { return _decMin; }
    double getDecMax() const { return _decMax; }

    /// Compute the area of this box in steradians.
    double area() const;

    /// Return the extent in right ascension of this box.
    double getRaExtent() const {
        if (wraps()) {
            return 360.0 - _raMin + _raMax;
        }
        return _raMax - _raMin;
    }

    /// Does this box contain the given spherical coordinates?
    bool contains(std::pair<double, double> const & position) const {
        if (position.second < _decMin || position.second > _decMax) {
            return false;
        }
        if (wraps()) {
           return position.first >= _raMin || position.first <= _raMax;
        }
        return position.first >= _raMin && position.first <= _raMax;
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

    /// Compute a conservative approximation to the list of HTM trangles
    /// potentially overlapping this box.
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


/** A Chunker assigns points to partitions using a simple scheme that
    breaks the unit sphere into fixed height declination stripes. These are
    in turn broken up into fixed width right ascension chunks (each stripe
    has a variable number of chunks to account for distortion at the poles).
    Chunks are in turn broken up into fixed height sub-stripes, and each
    sub-stripe is then divided into fixed width sub-chunks. As before,
    the number of sub-chunks per sub-stripe is variable to account for
    polar distortion.

    Also provided are methods for retrieving bounding boxes of chunks and
    sub-chunks, as well as for assigning chunks to (Qserv worker) nodes.
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

#endif // GEOMETRY_H
