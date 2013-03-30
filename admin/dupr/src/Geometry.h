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

/// \file
/// \brief Machinery for spherical geometry and
///        Hierarchical Triangular Mesh indexing.

#ifndef LSST_QSERV_ADMIN_DUPR_GEOMETRY_H
#define LSST_QSERV_ADMIN_DUPR_GEOMETRY_H

#include <stdint.h>
#include <cmath>
#include <algorithm>
#include <utility>
#include <vector>

#include "Vector.h"


namespace lsst { namespace qserv { namespace admin { namespace dupr {


/// Clamp `dec` to lie in the `[-90,90]` degree range.
inline double clampDec(double dec) {
    if (dec < -90.0) {
        return -90.0;
    } else if (dec > 90.0) {
        return 90.0;
    }
    return dec;
}

/// Return the minimum delta between two right ascensions,
/// both expected to be in degrees.
inline double minDeltaRa(double ra1, double ra2) {
    double delta = std::fabs(ra1 - ra2);
    return std::min(delta, 360.0 - delta);
}

/// Range reduce `ra` to lie in the `[0, 360)` degree range.
double reduceRa(double ra);

/// Compute the extent in right ascension `[-α,α]` of the circle
/// with radius `r` and center `(0, centerDec)` on the unit sphere.
/// Both `r` and `centerDec` are assumed to be in units of degrees;
/// `centerDec` is clamped to lie in `[-90, 90]` and `r` must
/// lie in `[0, 90]`.
double maxAlpha(double r, double centerDec);

/// Compute the HTM ID of `v`.
uint32_t htmId(Vector3d const &v, int level);

/// Return the HTM subdivision level of `id` or -1 if `id` is invalid.
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

/// Return the angular separation between `v0` and `v1` in radians.
double angSep(Vector3d const & v0, Vector3d const & v1);


class SphericalBox;

/// A triangle on the surface of the unit sphere with great-circle edges.
///
/// The main purpose of this class is to allow conversion between cartesian
/// 3-vectors and spherical barycentric coordinates.
///
/// The spherical barycentric coordinates b1, b2 and b3 of a 3-vector V,
/// given linearly independent triangle vertices V1, V2 and V3,
/// are defined as the solution to:
///
///     b1*V1 + b2*V2 + b3*V3 = V
///
/// If we let the column vector B = transpose([b1 b2 b3]) and M be the
/// 3x3 matrix with column vectors V1, V2 and V3, we can write the above
/// more simply as:
///
///     M * B = V
///
/// or
///
///     B = M⁻¹ * V
///
/// What are such coordinates used for?
///
/// Well, at a very high level, the Qserv data duplicator works by building a
/// map of non-empty HTM triangles. It converts the coordinates of each point
/// to spherical barycentric form. Then, to populate an empty triangle u, the
/// duplicator chooses a non-empty triangle v and copies all its points.
/// For a point V in v, the position of the copy is set to
///
///     Mᵤ * (Mᵥ⁻¹ * V) = (Mᵤ * Mᵥ⁻¹) * V
///
/// In other words, V is transformed by the matrix that maps the vertices of
/// v to the vertices of u. Since the area and proportions of different HTM
/// triangles don't vary all that much, one can think of (Mᵤ * Mᵥ⁻¹) as
/// something fairly close to a rotation. The fact that the transform isn't
/// quite length preserving doesn't matter; after all, cartesian coordinates
/// V and k*V (k > 0) map to the same spherical coordinates. Unlike an
/// approach that shifts around copies of an input data set in spherical
/// coordinate space, there are no serious distortion issues to worry about
/// near the poles.
///
/// Note that if the subdivision level of the target triangles is different
/// from that of the source triangles, the transform above can be used to
/// derive a catalog of greater or smaller density from an input catalog,
/// with relative angular structure roughly preserved.
class SphericalTriangle {
public:
    /// Construct the HTM triangle with the given HTM ID.
    explicit SphericalTriangle(uint32_t htmId);

    /// Construct the triangle with the given vertices.
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
    Matrix3d const & getCartesianTransform() const {
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
    /// [V0 V1 V2], where column vectors V0, V1, V2 are the triangle
    /// vertices (unit vectors).
    Matrix3d _m;
    /// Inverse of _m, corresponding to
    /// transpose([V1 x V2, V2 x V0, V0 x V1])/det(_m).
    Matrix3d _mi;
};


/// A spherical coordinate space bounding box.
///
/// This is similar to a bounding box in cartesian space in that it is
/// specified by a pair of points; however, a spherical box may correspond
/// to the entire unit-sphere, a spherical cap, a lune or the traditional
/// rectangle. Additionally, spherical boxes can span the 0/360 degree
/// right ascension angle discontinuity.
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

    /// Does the box wrap around the 0/360 degree right ascension discontinuity?
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
    bool contains(double ra, double dec) const {
        if (dec < _decMin || dec > _decMax) {
            return false;
        }
        if (wraps()) {
           return ra >= _raMin || ra <= _raMax;
        }
        return ra >= _raMin && ra <= _raMax;
    }

    bool contains(std::pair<double, double> const & position) const {
        return contains(position.first, position.second);
    }

    /// Does this box intersect the given box?
    bool intersects(SphericalBox const & box) const {
        if (isEmpty() || box.isEmpty()) {
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

    /// Compute a conservative approximation to the list of HTM triangles
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

}}}} // namespace lsst::qserv::admin::dupr

#endif // LSST_QSERV_ADMIN_DUPR_GEOMETRY_H
