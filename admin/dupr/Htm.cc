#include "Htm.h"

#include <cmath>
#include <cstdio>
#include <algorithm>
#include <stdexcept>

#include "FileUtils.h"

using std::sin;
using std::cos;
using std::atan2;
using std::sqrt;
using std::min;
using std::max;
using std::string;
using std::swap;
using std::printf;

using Eigen::Vector2d;
using Eigen::Vector3d;


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
    double delta = abs(ra1 - ra2);
    return min(delta, 360.0 - delta);
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
    if (abs(d) + r > 90.0 - 1/3600.0) {
        return 180.0;
    }
    r *= RAD_PER_DEG;
    d *= RAD_PER_DEG;
    double y = sin(r);
    double x = sqrt(abs(cos(d - r) * cos(d + r)));
    return DEG_PER_RAD * abs(atan(y / x));
}

// Compute the number of segments to divide the given declination range
// (stripe) into. Two points in the declination range separated by at least
// one segment are guaranteed to be separated by an angular distance of at
// least width.
int segments(double decMin, double decMax, double width) {
    double dec = max(abs(decMin), abs(decMax));
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
    double y = sqrt(abs(u * u - x * x));
    return static_cast<int>(
        floor(360.0 / abs(DEG_PER_RAD * atan2(y, x))));
}

// Return the angular width of a single segment obtained by
// chopping the declination stripe [decMin, decMax] into
// numSegments equal width (in right ascension) segments.
double segmentWidth(double decMin, double decMax, int numSegments) {
    double dec = max(abs(decMin), abs(decMax)) * RAD_PER_DEG;
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
    uint32_t x = (id >> 1);
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

} // namespace dupr
