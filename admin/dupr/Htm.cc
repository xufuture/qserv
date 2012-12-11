#include "Htm.h"

#include <cmath>
#include <cstdio>
#include <algorithm>
#include <stdexcept>

#include "FileUtils.h"

using std::fabs;
using std::sin;
using std::cos;
using std::atan2;
using std::sqrt;
using std::min;
using std::max;
using std::printf;
using std::string;
using std::swap;
using std::vector;

using Eigen::Vector2d;
using Eigen::Vector3d;
using Eigen::Matrix3d;


namespace {

/*
    HTM triangles are subdivided into 4 sub-triangles as follows :

            v2
             *
            / \
           /   \
      sv1 *-----* sv0
         / \   / \
        /   \ /   \
    v0 *-----*-----* v1
            sv2

     -  vertices are unit magnitude 3-vectors
     -  edges are great circles on the unit sphere
     -  vertices are stored in counter-clockwise order
       (when viewed from outside the unit sphere in a
       right handed coordinate system)
     -  sv0 = (v1 + v2) / ||v1 + v2||, and likewise for sv1, sv2

    Note that if the HTM triangle given by (v0,v1,v2) has index I, then:
     -  sub triangle T0 = (v0,sv2,sv1) has index I*4
     -  sub triangle T1 = (v1,sv0,sv2) has index I*4 + 1
     -  sub triangle T2 = (v2,sv1,sv0) has index I*4 + 2
     -  sub triangle T3 = (sv0,sv1,sv2) has index I*4 + 3

    All HTM triangles are obtained via subdivision of 8 initial
    triangles, defined from the following set of 6 vertices :
     -  V0 = ( 0,  0,  1) north pole
     -  V1 = ( 1,  0,  0)
     -  V2 = ( 0,  1,  0)
     -  V3 = (-1,  0,  0)
     -  V4 = ( 0, -1,  0)
     -  V5 = ( 0,  0, -1) south pole

    The root triangles (corresponding to subdivision level 0) are :
     -  S0 = (V1, V5, V2), HTM index = 8
     -  S1 = (V2, V5, V3), HTM index = 9
     -  S2 = (V3, V5, V4), HTM index = 10
     -  S3 = (V4, V5, V1), HTM index = 11
     -  N0 = (V1, V0, V4), HTM index = 12
     -  N1 = (V4, V0, V3), HTM index = 13
     -  N2 = (V3, V0, V2), HTM index = 14
     -  N3 = (V2, V0, V1), HTM index = 15

    'S' denotes a triangle in the southern hemisphere,
    'N' denotes a triangle in the northern hemisphere.
*/

double const DEG_PER_RAD = 57.2957795130823208767981548141;
double const RAD_PER_DEG = 0.0174532925199432957692369076849;
double const EPSILON = 0.001 / 3600;

// HTM root triangle numbers. Add 8 to obtain a level 0 HTM ID.
uint32_t const S0 = 0;
uint32_t const S1 = 1;
uint32_t const S2 = 2;
uint32_t const S3 = 3;
uint32_t const N0 = 4;
uint32_t const N1 = 5;
uint32_t const N2 = 6;
uint32_t const N3 = 7;

Vector3d const X(1.0, 0.0, 0.0);
Vector3d const Y(0.0, 1.0, 0.0);
Vector3d const Z(0.0, 0.0, 1.0);

Vector3d const NX(-1.0,  0.0,  0.0);
Vector3d const NY( 0.0, -1.0,  0.0);
Vector3d const NZ( 0.0,  0.0, -1.0);

// Vertex triplet for each HTM root triangle.
Vector3d const * const htmRootVert[24] = {
    &X,  &NZ, &Y,  // S0
    &Y,  &NZ, &NX, // S1
    &NX, &NZ, &NY, // S2
    &NY, &NZ, &X,  // S3
    &X,  &Z,  &NY, // N0
    &NY, &Z,  &NX, // N1
    &NX, &Z,  &Y,  // N2
    &Y,  &Z,  &X   // N3
};

// Edge normal triplet for each HTM root triangle.
Vector3d const * const htmRootEdge[24] = {
    &Y,  &X,  &NZ, // S0
    &NX, &Y,  &NZ, // S1
    &NY, &NX, &NZ, // S2
    &X,  &NY, &NZ, // S3
    &NY, &X,  &Z,  // N0
    &NX, &NY, &Z,  // N1
    &Y,  &NX, &Z,  // N2
    &X,  &Y,  &Z   // N3
};

// Return the number of the HTM root triangle containing v.
inline uint32_t rootNumFor(Vector3d const &v)
{
    if (v(2) < 0.0) {
        // S0, S1, S2, S3
        if (v(1) > 0.0) {
            return (v(0) > 0.0) ? S0 : S1;
        } else if (v(1) == 0.0) {
            return (v(0) >= 0.0) ? S0 : S2;
        } else {
            return (v(0) < 0.0) ? S2 : S3;
        }
    } else {
        // N0, N1, N2, N3
        if (v(1) > 0.0) {
            return (v(0) > 0.0) ? N3 : N2;
        } else if (v(1) == 0.0) {
            return (v(0) >= 0.0) ? N3 : N1;
        } else {
            return (v(0) < 0.0) ? N1 : N0;
        }
    }
}

// Clamp dec to lie in range [-90,90]
inline double clampDec(double dec) {
    if (dec < -90.0) {
        return -90.0;
    } else if (dec > 90.0) {
        return 90.0;
    }
    return dec;
}

// Return minimum delta between 2 right ascensions
inline double minDeltaRa(double ra1, double ra2) {
    double delta = fabs(ra1 - ra2);
    return min(delta, 360.0 - delta);
}

// Range reduce the given right ascension to lie in [0, 360).
double reduceRa(double ra) {
    ra = fmod(ra, 360.0);
    if (ra < 0.0) {
        ra += 360.0;
        if (ra == 360.0) {
            ra = 0.0;
        }
    }
    return ra;
}

// Compute the extent in longitude [-alpha,alpha] of the circle
// with radius r and center (0, centerDec) on the unit sphere.
// Both r and centerDec are assumed to be in units of degrees;
// centerDec is clamped to lie in the range [-90,90] and r must
// lie in the range [0, 90].
double maxAlpha(double r, double centerDec) {
    if (r < 0.0 || r > 90.0) {
        throw std::runtime_error("radius must lie in range [0, 90] deg");
    }
    if (r == 0.0) {
        return 0.0;
    }
    double d = clampDec(centerDec);
    if (fabs(d) + r > 90.0 - 1/3600.0) {
        return 180.0;
    }
    r *= RAD_PER_DEG;
    d *= RAD_PER_DEG;
    double y = sin(r);
    double x = sqrt(fabs(cos(d - r) * cos(d + r)));
    return DEG_PER_RAD * fabs(atan(y / x));
}

// Compute the number of segments to divide the given declination range
// (stripe) into. Two points in the declination range separated by at least
// one segment are guaranteed to be separated by an angular distance of at
// least width.
int segments(double decMin, double decMax, double width) {
    double dec = max(fabs(decMin), fabs(decMax));
    if (dec > 90.0 - 1/3600.0) {
        return 1;
    }
    if (width >= 180.0) {
        return 1;
    } else if (width < 1/3600.0) {
        width = 1/3600;
    }
    dec *= RAD_PER_DEG;
    double cw = cos(width * RAD_PER_DEG);
    double sd = sin(dec);
    double cd = cos(dec);
    double x = cw - sd * sd;
    double u = cd * cd;
    double y = sqrt(fabs(u * u - x * x));
    return static_cast<int>(
        floor(360.0 / fabs(DEG_PER_RAD * atan2(y, x))));
}

// Return the angular width of a single segment obtained by
// chopping the declination stripe [decMin, decMax] into
// numSegments equal width (in right ascension) segments.
double segmentWidth(double decMin, double decMax, int numSegments) {
    double dec = max(fabs(decMin), fabs(decMax)) * RAD_PER_DEG;
    double cw = cos(RAD_PER_DEG * (360.0 / numSegments));
    double sd = sin(dec);
    double cd = cos(dec);
    return acos(cw * cd * cd + sd * sd) * DEG_PER_RAD;
}

// For use in computing partition bounds: clamps an input longitude angle
// to 360.0 deg. Any input angle >= 360.0 - EPSILON is mapped to 360.0.
// This is because partition bounds are computed by multiplying a sub-chunk
// width by a sub-chunk number; the last sub-chunk in a sub-stripe can
// therefore have a maximum longitude angle very slightly less than 360.0.
double clampRa(double ra) {
    if (ra >= 360.0 or (360.0 - ra < EPSILON)) {
        return 360.0;
    }
    return ra;
}

// Return the angular separation between v0 and v1 in degrees.
double angSep(Vector3d const & v0, Vector3d const & v1) {
    double cs = v0.dot(v1);
    Vector3d n = v0.cross(v1);
    double ss = n.norm();
    if (cs == 0.0 && ss == 0.0) {
        return 0.0;
    }
    return atan2(ss, cs) * DEG_PER_RAD;
}

// 32 bit integer hash function from Brett Mulvey.
// See http://home.comcast.net/~bretm/hash/4.html
uint32_t mulveyHash(uint32_t x) {
    x += x << 16;
    x ^= x >> 13;
    x += x << 4;
    x ^= x >> 7;
    x += x << 10;
    x ^= x >> 5;
    x += x << 8;
    x ^= x >> 16;
    return x;
}

} // unnamed namespace


namespace dupr {

uint32_t htmId(Vector3d const &v, int level) {
    if (level < 0 || level > HTM_MAX_LEVEL) {
        throw std::runtime_error("invalid HTM subdivision level");
    }
    uint32_t id = rootNumFor(v);
    if (level == 0) {
        return id + 8;
    }
    Vector3d v0(*htmRootVert[id*3]);
    Vector3d v1(*htmRootVert[id*3 + 1]);
    Vector3d v2(*htmRootVert[id*3 + 2]);
    id += 8;
    for (; level != 0; --level) {
        Vector3d const sv1 = (v2 + v0).normalized();
        Vector3d const sv2 = (v0 + v1).normalized();
        if (v.dot((sv1 + sv2).cross(sv1 - sv2)) >= 0) {
            v1 = sv2;
            v2 = sv1;
            id = id << 2;
            continue;
        }
        Vector3d const sv0 = (v1 + v2).normalized();
        if (v.dot((sv2 + sv0).cross(sv2 - sv0)) >= 0) {
            v0 = v1;
            v1 = sv0;
            v2 = sv2;
            id = (id << 2) + 1;
            continue;
        }
        if (v.dot((sv0 + sv1).cross(sv0 - sv1)) >= 0) {
            v0 = v2;
            v1 = sv1;
            v2 = sv0;
            id = (id << 2) + 2;
        } else {
            v0 = sv0;
            v1 = sv1;
            v2 = sv2;
            id = (id << 2) + 3;
        }
    }
    return id;
}


int htmLevel(uint32_t id) {
    if (id < 8) {
        return -1; // invalid ID
    }
    // set x = 2^(i + 1) - 1, where i is the index of the MSB of id.
    uint32_t x = id;
    x |= (x >> 1);
    x |= (x >> 2);
    x |= (x >> 4);
    x |= (x >> 8);
    x |= (x >> 16);
    // compute bit population count p of x; the HTM level is (p - 4) / 2;
    // see http://en.wikipedia.org/wiki/Hamming_weight
    uint32_t const m1 = 0x55555555;
    uint32_t const m2 = 0x33333333;
    uint32_t const m4 = 0x0f0f0f0f;
    uint32_t const h01 = 0x01010101;
    x -= (x >> 1) & m1;
    x = (x & m2) + ((x >> 2) & m2);
    x = (x + (x >> 4)) & m4;
    uint32_t level = ((x * h01) >> 24) - 4;
    // check that level is even, in range and that the 4 MSBs
    // of id are between 8 and 15.
    if ((level & 1) != 0 || ((id >> level) & 0x8) == 0 || level > HTM_MAX_LEVEL*2) {
        return -1;
    }
    return static_cast<int>(level >> 1);
}


Vector3d const cartesian(Vector2d const &radec) {
    double ra = radec(0) * RAD_PER_DEG;
    double dec = radec(1) * RAD_PER_DEG;
    double sinRa = sin(ra);
    double cosRa = cos(ra);
    double sinDec = sin(dec);
    double cosDec = cos(dec);
    return Vector3d(cosRa * cosDec, sinRa * cosDec, sinDec);
}


Vector2d const spherical(Vector3d const &v) {
    Vector2d sc = Vector2d::Zero();
    double d2 = v(0)*v(0) + v(1)*v(1);
    if (d2 != 0.0) {
        double ra = atan2(v(1), v(0)) * DEG_PER_RAD;
        if (ra < 0.0) {
            ra += 360.0;
            if (ra == 360.0) {
                ra = 0.0;
            }
        }
        sc(0) = ra;
    }
    if (v(2) != 0.0) {
        sc(1) = clampDec(atan2(v(2), sqrt(d2)) * DEG_PER_RAD);
    }
    return sc;
}


Trixel::Trixel(uint32_t id) : _m(), _mi() {
    int level = htmLevel(id);
    if (level < 0) {
        throw std::runtime_error("Invalid HTM ID passed to Trixel constructor");
    }
    uint32_t r = (id >> (level * 2)) - 8;
    Vector3d v0(*htmRootVert[r*3]);
    Vector3d v1(*htmRootVert[r*3 + 1]);
    Vector3d v2(*htmRootVert[r*3 + 2]);
    // subdivide, maintaining triangle vertices as we go
    for (--level; level >= 0; --level) {
        uint32_t child = (id >> (level * 2)) & 0x3;
        Vector3d const sv0 = (v1 + v2).normalized();
        Vector3d const sv1 = (v2 + v0).normalized();
        Vector3d const sv2 = (v0 + v1).normalized();
        switch (child) {
        case 0:
            v1 = sv2;
            v2 = sv1;        
            break;
        case 1:
            v0 = v1;
            v1 = sv0;
            v2 = sv2;
            break;
        case 2:
            v0 = v2;
            v1 = sv1;
            v2 = sv0;
            break;
        case 3:
            v0 = sv0;
            v1 = sv1;
            v2 = sv2;
            break;
        }
    }
    // set column vectors of _m to trixel vertices
    _m.col(0) = v0;
    _m.col(1) = v1;
    _m.col(2) = v2;
    _mi = _m.inverse();
}


PopulationMap::PopulationMap(int level) : 
    _numTrix(static_cast<uint32_t>(8) << (2*level)),
    _level(level),
    _queryable(false),
    _count(new uint64_t[_numTrix + 1]()),
    _offset(new uint64_t[_numTrix + 1]()),
    _nonEmpty()
{
    if (level < 0 || level > HTM_MAX_LEVEL) {
        throw std::runtime_error("invalid HTM subdivision level");
    }
}

PopulationMap::PopulationMap(string const & path) :
    _numTrix(0),
    _level(-1),
    _queryable(false),
    _count(),
    _offset(),
    _nonEmpty()
{
    InputFile f(path);
    if (f.size() % static_cast<off_t>(sizeof(uint32_t)) != 0 ||
        f.size() < static_cast<off_t>(2 * sizeof(uint32_t))) {
        throw std::runtime_error("invalid population map file");
    }
    boost::scoped_array<uint32_t> data(new uint32_t[f.size() / sizeof(uint32_t)]);
    f.read(data.get(), 0, f.size());
    _numTrix = data[0];
    // the number of trixels at level L is equal to the smallest HTM ID of level L
    _level = htmLevel(_numTrix);
    if (_level < 0) {
        throw std::runtime_error("invalid population map file");
    }
    uint32_t n = data[1];
    if (f.size() != static_cast<off_t>(sizeof(uint32_t) * (3*n + 2)) ||
        n > _numTrix) {
        throw std::runtime_error("invalid population map file");
    }
    _nonEmpty.reserve(n);
    _count.reset(new uint64_t[_numTrix + 1]());
    _offset.reset(new uint64_t[_numTrix + 1]());
    for (uint32_t i = 0; i < n; ++i) {
        uint32_t id = data[3*i + 2];
        _nonEmpty.push_back(id);
        _count[id - _numTrix + 1] = data[3*i + 3];
        _offset[id - _numTrix + 1] = data[3*i + 4];
    }
    data.reset();
    makeQueryable();
}

PopulationMap::~PopulationMap() { }

void PopulationMap::makeQueryable() {
    if (_queryable) {
        return; // nothing to do
    }
    size_t n = 0;
    for (uint32_t i = 1; i <= _numTrix; ++i) {
        if (_count[i] != 0) {
            ++n;
        }
        if (_count[i] > 0xFFFFFFFF) {
            throw std::runtime_error(
                "Trixel contains more than 2^32 - 1 records. "
                "HTM subdivision level must be increased.");
        }
        if (_offset[i] > 0xFFFFFFFF) {
            throw std::runtime_error(
                "Trixel data is larger than 2^32 - 1 bytes. "
                "HTM subdivision level must be increased.");
        }
    }
    _nonEmpty.reserve(n);
    for (uint32_t i = 1; i <= _numTrix; ++i) {
        if (_count[i] != 0) {
            _nonEmpty.push_back(i - 1 + _numTrix);
        }
        _count[i] += _count[i - 1];
        _offset[i] += _offset[i - 1];
    }
    _queryable = true;
}

void PopulationMap::write(string const & path) const {
    if (!_queryable) {
        throw std::runtime_error(
            "cannot serialize a population map that is still in-construction");
    }
    size_t const n = 3*_nonEmpty.size() + 2;
    boost::scoped_array<uint32_t> buf(new uint32_t[n]);
    buf[0] = _numTrix;
    buf[1] = static_cast<uint32_t>(_nonEmpty.size());
    for (size_t i = 0; i < _nonEmpty.size(); ++i) {
        uint32_t id = _nonEmpty[i];
        uint32_t nrec = getNumRecords(id);
        uint32_t sz = getSize(id);
        buf[3*i + 2] = id;
        buf[3*i + 3] = nrec;
        buf[3*i + 4] = sz;
        printf("Trixel %9u : %7u records, %9u bytes\n", id, nrec, sz);
    }
    OutputFile f(path);
    f.append(buf.get(), n * sizeof(uint32_t));
}


SphericalBox::SphericalBox(double raMin, double raMax, double decMin, double decMax) {
    if (decMin > decMax) {
        throw std::runtime_error("Dec max < Dec min");
    } else if (raMax < raMin && (raMax < 0.0 || raMin > 360.0)) {
        throw std::runtime_error("RA max < RA min");
    }
    if (raMax - raMin >= 360.0) {
        _raMin = 0.0;
        _raMax = 360.0;
    } else {
        _raMin = reduceRa(raMin);
        _raMax = reduceRa(raMax);
    }
    _decMin = clampDec(decMin);
    _decMax = clampDec(decMax);
}

SphericalBox::SphericalBox(Matrix3d const & m) {
    // find bounding circle for triangle with center cv and radius r
    Vector3d cv = m.col(0) + m.col(1) + m.col(2);
    double r = angSep(cv, m.col(0));
    r = max(r, angSep(cv, m.col(1)));
    r = max(r, angSep(cv, m.col(2)));
    r += 1/3600.0;
    // construct bounding box for bounding circle. This is inexact,
    // but involves less code than a more accurate computation.
    Vector2d c = spherical(cv);
    double alpha = maxAlpha(r, c(1));
    _decMin = clampDec(c(1) - r);
    _decMax = clampDec(c(1) + r);
    if (alpha > 180.0 - 1/3600.0) {
        _raMin = 0.0;
        _raMax = 360.0;
    } else {
        double raMin = c(0) - alpha;
        double raMax = c(0) + alpha;
        if (raMin < 0.0) {
            raMin += 360.0;
            if (raMin == 360.0) {
                raMin = 0.0;
            }
        }
        _raMin = raMin;
        if (raMax > 360.0) {
            raMax -= 360.0;
        }
        _raMax = raMax;
    }
}

void SphericalBox::expand(double radius) {
    if (radius < 0.0) {
        throw std::runtime_error(
            "Cannot expand spherical box by a negative angle");
    } else if (radius == 0.0) {
        return;
    }
    double const extent = getRaExtent();
    double const alpha = maxAlpha(radius, max(fabs(_decMin), fabs(_decMax)));
    if (extent + 2.0 * alpha >= 360.0 - 1/3600.0) {
        _raMin = 0.0;
        _raMax = 360.0;
    } else {
        _raMin -= alpha;
        if (_raMin < 0.0) {
            _raMin += 360.0;
            if (_raMin == 360.0) {
                _raMin = 0.0;
            }
        }
        _raMax += alpha;
        if (_raMax > 360.0) {
            _raMax -= 360.0;
        }
    }
    _decMin = clampDec(_decMin - radius);
    _decMax = clampDec(_decMax + radius);
}

vector<uint32_t> const SphericalBox::htmIds(int level) const {
    vector<uint32_t> ids;
    if (level < 0 || level > HTM_MAX_LEVEL) {
        throw std::runtime_error("invalid HTM subdivision level");
    }
    for (int r = 0; r < 8; ++r) {
        Matrix3d m;
        m.col(0) = *htmRootVert[r*3];
        m.col(1) = *htmRootVert[r*3 + 1];
        m.col(2) = *htmRootVert[r*3 + 2];
        findIds(r + 8, level, m, ids);
    }
    return ids;
}

// Slow method of finding trixels overlapping a box. But, for the subdivision
// levels and box sizes encountered in practice, this is very unlikely to be
// a performance problem.
void SphericalBox::findIds(
    uint32_t id,
    int level,
    Matrix3d const & m,
    vector<uint32_t> & ids
) const {
    if (!intersects(SphericalBox(m))) {
        return;
    } else if (level == 0) {
        ids.push_back(id);
        return;
    }
    Matrix3d mChild;
    Vector3d const sv0 = (m.col(1) + m.col(2)).normalized();
    Vector3d const sv1 = (m.col(2) + m.col(0)).normalized();
    Vector3d const sv2 = (m.col(0) + m.col(1)).normalized();
    mChild.col(0) = m.col(0);
    mChild.col(1) = sv2;
    mChild.col(2) = sv1;
    findIds(id*4, level - 1, mChild, ids);
    mChild.col(0) = m.col(1);
    mChild.col(1) = sv0;
    mChild.col(2) = sv2;
    findIds(id*4 + 1, level - 1, mChild, ids);
    mChild.col(0) = m.col(2);
    mChild.col(1) = sv1;
    mChild.col(2) = sv0;
    findIds(id*4 + 2, level - 1, mChild, ids);
    mChild.col(0) = sv0;
    mChild.col(1) = sv1;
    mChild.col(2) = sv2;
    findIds(id*4 + 3, level - 1, mChild, ids);
}


Chunker::Chunker(double overlap, int32_t numStripes, int32_t numSubStripesPerStripe) :
    _numChunksPerStripe(),
    _numSubChunksPerChunk(),
    _subChunkWidth(),
    _alpha()
{
    if (numStripes < 1 || numSubStripesPerStripe < 1) {
        throw std::runtime_error(
            "Number of stripes and sub-stripes per stripe must be positive");
    }
    if (overlap < 0.0 || overlap > 10.0) {
        throw std::runtime_error("Overlap must be in range [0, 10] deg");
    }
    int32_t const numSubStripes = numStripes * numSubStripesPerStripe;
    _overlap = overlap;
    _numStripes = numStripes;
    _numSubStripesPerStripe = numSubStripesPerStripe;
    double const stripeHeight = 180.0 / numStripes;
    double const subStripeHeight = 180.0 / numSubStripes;
    if (subStripeHeight < overlap) {
        throw std::runtime_error("Overlap exceeds sub-stripe height");
    }
    _subStripeHeight = subStripeHeight;
    boost::scoped_array<int32_t> numChunksPerStripe(new int32_t[numStripes]);
    boost::scoped_array<int32_t> numSubChunksPerChunk(new int32_t[numSubStripes]);
    boost::scoped_array<double> subChunkWidth(new double[numSubStripes]);
    boost::scoped_array<double> alpha(new double[numSubStripes]);
    int32_t maxSubChunksPerChunk = 0;
    for (int32_t i = 0; i < numStripes; ++i) {
        int32_t nc = segments(
            i*stripeHeight - 90.0, (i + 1)*stripeHeight - 90.0, stripeHeight);
        numChunksPerStripe[i] = nc;
        for (int32_t j = 0; j < numSubStripesPerStripe; ++j) {
            int32_t ss = i * numSubStripesPerStripe + j;
            double decMin = ss*subStripeHeight - 90.0;
            double decMax = (ss + 1)*subStripeHeight - 90.0;
            int32_t nsc = segments(decMin, decMax, subStripeHeight) / nc;
            maxSubChunksPerChunk = max(maxSubChunksPerChunk, nsc);
            numSubChunksPerChunk[ss] = nsc;
            double scw = 360.0 / (nsc * nc);
            subChunkWidth[ss] = scw;
            double a = maxAlpha(overlap, max(fabs(decMin), fabs(decMax)));
            if (a > scw) {
                throw std::runtime_error("Overlap exceeds sub-chunk width");
            }
            alpha[ss] = a;
        }
    }
    _maxSubChunksPerChunk = maxSubChunksPerChunk;
    swap(numChunksPerStripe, _numChunksPerStripe);
    swap(numSubChunksPerChunk, _numSubChunksPerChunk);
    swap(subChunkWidth, _subChunkWidth);
    swap(alpha, _alpha);
}

Chunker::~Chunker() { }

SphericalBox const Chunker::getChunkBounds(int32_t chunkId) const {
    int32_t stripe = getStripe(chunkId);
    int32_t chunk = getChunk(chunkId, stripe);
    double width = 360.0 / _numChunksPerStripe[stripe];
    double raMin = max(0.0, chunk*width);
    double raMax = clampRa((chunk + 1)*width);
    double decMin = clampDec(stripe*_numSubStripesPerStripe*_subStripeHeight - 90.0);
    double decMax = clampDec((stripe + 1)*_numSubStripesPerStripe*_subStripeHeight - 90.0);
    return SphericalBox(raMin, raMax, decMin, decMax);
}

SphericalBox const Chunker::getSubChunkBounds(
    int32_t chunkId,
    int32_t subChunkId
) const {
    int32_t stripe = getStripe(chunkId);
    int32_t chunk = getChunk(chunkId, stripe);
    int32_t subStripe = getSubStripe(subChunkId, stripe);
    int32_t subChunk = getSubChunk(subChunkId, stripe, subStripe, chunk);
    double raMin = subChunk*_subChunkWidth[subChunk];
    double raMax = clampRa((subChunk + 1)*_subChunkWidth[subChunk]);
    double decMin = clampDec(subStripe*_subStripeHeight - 90.0);
    double decMax = clampDec((subStripe + 1)*_subStripeHeight - 90.0);
    return SphericalBox(raMin, raMax, decMin, decMax);
}

void Chunker::locate(
    Vector2d const & position,
    int32_t chunkId,
    vector<ChunkLocation> & locations
) const {
    // Find non-overlap location of position
    double const ra = position(0);
    double const dec = position(1);
    int32_t subStripe = static_cast<int32_t>(
        floor((dec + 90.0) / _subStripeHeight));
    int32_t const numSubStripes = _numSubStripesPerStripe*_numStripes;
    if (subStripe >= numSubStripes) {
        subStripe = numSubStripes - 1;
    }
    int32_t const stripe = subStripe/_numSubStripesPerStripe;
    int32_t subChunk = static_cast<int32_t>(
        floor(ra / _subChunkWidth[subStripe]));
    int32_t const numChunks = _numChunksPerStripe[stripe];
    int32_t const numSubChunksPerChunk = _numSubChunksPerChunk[subStripe];
    int32_t const numSubChunks = numChunks*numSubChunksPerChunk;
    if (subChunk >= numSubChunks) {
        subChunk = numSubChunks - 1;
    }
    int32_t const chunk = subChunk / numSubChunksPerChunk;
    if (chunkId < 0 || getChunkId(stripe, chunk) == chunkId) {
        // non-overlap location is in the requested chunk
        ChunkLocation loc;
        loc.chunkId = getChunkId(stripe, chunk);
        loc.subChunkId = getSubChunkId(stripe, subStripe, chunk, subChunk);
        loc.overlap = ChunkLocation::CHUNK;
        locations.push_back(loc);
    }
    if (_overlap == 0.0) {
        return;
    }
    // Get sub-chunk bounds
    double const raMin = subChunk*_subChunkWidth[subChunk];
    double const raMax = clampRa((subChunk + 1)*_subChunkWidth[subChunk]);
    double const decMin = clampDec(subStripe*_subStripeHeight - 90.0);
    double const decMax = clampDec((subStripe + 1)*_subStripeHeight - 90.0);
    // Check whether the position is in the overlap regions of sub-chunks in
    // the sub-stripe above and below this one.
    if (subStripe > 0 && dec < decMin + _overlap) {
        // position is in full-overlap region of sub-chunks 1 sub-stripe down
        upDownOverlap(ra, chunkId, ChunkLocation::FULL_OVERLAP,
                      (subStripe - 1) / _numSubStripesPerStripe,
                      subStripe - 1, locations);
    }
    if (subStripe < numSubStripes - 1 && dec >= decMax - _overlap) {
        // position is in full/self-overlap regions of sub-chunks 1 sub-stripe up
        upDownOverlap(ra, chunkId, ChunkLocation::SELF_OVERLAP,
                      (subStripe + 1) / _numSubStripesPerStripe,
                      subStripe + 1, locations);
    }
    // Check whether the position is in the overlap regions of the sub-chunks
    // to the left and right.
    if (numSubChunks == 1) {
        return;
    }
    double const alpha = _alpha[subStripe];
    if (ra < raMin + alpha) {
        // position is in full/self-overlap region of sub-chunk to the left
        int32_t overlapChunk, overlapSubChunk;
        if (subChunk == 0) {
            overlapChunk = numChunks - 1;
            overlapSubChunk = numSubChunks - 1;
        } else {
            overlapChunk = (subChunk - 1) / numSubChunksPerChunk;
            overlapSubChunk = subChunk - 1;
        }
        if (chunkId < 0 || getChunkId(stripe, overlapChunk) == chunkId) {
            ChunkLocation loc;
            loc.chunkId = getChunkId(stripe, overlapChunk);
            loc.subChunkId = getSubChunkId(
                stripe, subStripe, overlapChunk, overlapSubChunk);
            loc.overlap = ChunkLocation::SELF_OVERLAP;
            locations.push_back(loc);
        }
    }
    if (ra > raMax - alpha) {
        // position is in full-overlap region of sub-chunk to the right
        int32_t overlapChunk, overlapSubChunk;
        if (subChunk == numSubChunks - 1) {
            overlapChunk = 0;
            overlapSubChunk = 0;
        } else {
            overlapChunk = (subChunk + 1) / numSubChunksPerChunk;
            overlapSubChunk = subChunk + 1;
        }
        if (chunkId < 0 || getChunkId(stripe, overlapChunk) == chunkId) {
            ChunkLocation loc;
            loc.chunkId = getChunkId(stripe, overlapChunk);
            loc.subChunkId = getSubChunkId(
                stripe, subStripe, overlapChunk, overlapSubChunk);
            loc.overlap = ChunkLocation::FULL_OVERLAP;
            locations.push_back(loc);
        }
    }
}

void Chunker::upDownOverlap(
    double ra,
    int32_t chunkId,
    ChunkLocation::Overlap overlap,
    int32_t stripe,
    int32_t subStripe,
    vector<ChunkLocation> & locations
) const {
    int32_t const numChunks = _numChunksPerStripe[stripe];
    int32_t const numSubChunksPerChunk = _numSubChunksPerChunk[subStripe];
    int32_t const numSubChunks = numChunks*numSubChunksPerChunk;
    double const subChunkWidth = _subChunkWidth[subStripe];
    double const alpha = _alpha[subStripe];
    int32_t minSubChunk = static_cast<int32_t>(floor((ra - alpha) / subChunkWidth));
    int32_t maxSubChunk = static_cast<int32_t>(floor((ra + alpha) / subChunkWidth));
    if (minSubChunk < 0) {
        minSubChunk += numSubChunks;
    }
    if (maxSubChunk >= numSubChunks) {
        maxSubChunk -= numSubChunks;
    }
    if (minSubChunk > maxSubChunk) {
        // 0/360 wrap around
        for (int32_t subChunk = minSubChunk; subChunk < numSubChunks; ++subChunk) {
            int32_t chunk = subChunk / numSubChunksPerChunk;
            if (chunkId < 0 || getChunkId(stripe, chunk) == chunkId) {
                ChunkLocation loc;
                loc.chunkId = getChunkId(stripe, chunk);
                loc.subChunkId = getSubChunkId(stripe, subStripe, chunk, subChunk);
                loc.overlap = overlap;
                locations.push_back(loc);
            }
        }
        minSubChunk = 0;
    }
    for (int32_t subChunk = minSubChunk; subChunk <= maxSubChunk; ++subChunk) {
        int32_t chunk = subChunk / numSubChunksPerChunk;
        if (chunkId < 0 || getChunkId(stripe, chunk) == chunkId) {
            ChunkLocation loc;
            loc.chunkId = getChunkId(stripe, chunk);
            loc.subChunkId = getSubChunkId(stripe, subStripe, chunk, subChunk);
            loc.overlap = overlap;
            locations.push_back(loc);
        }
    }
}

vector<int32_t> const Chunker::getChunksFor(
    SphericalBox const & region,
    uint32_t node,
    uint32_t numNodes,
    bool hashChunks
) const {
    if (numNodes == 0) {
        throw std::runtime_error("There must be at least one node to assign chunks to");
    }
    if (node >= numNodes) {
        throw std::runtime_error("Node number must be in range [0, numNodes)");
    }
    vector<int32_t> chunks;
    uint32_t n = 0;
    // The slow and easy route - loop over every chunk, see if it belongs to
    // the given node, and if it also intersects with region, return it.
    for (int32_t stripe = 0; stripe < _numStripes; ++stripe) {
        for (int32_t chunk = 0; chunk < _numChunksPerStripe[stripe]; ++chunk, ++n) {
            int32_t const chunkId = getChunkId(stripe, chunk);
            if (hashChunks) {
                if (mulveyHash(static_cast<uint32_t>(chunkId)) % numNodes != node) {
                    continue;
                }
            } else {
                if (n % numNodes != node) {
                    continue;
                }
            }
            SphericalBox box = getChunkBounds(getChunkId(stripe, chunk));
            if (region.intersects(box)) {
                chunks.push_back(chunkId);
            }
        }
    }
    return chunks;
}

} // namespace dupr
