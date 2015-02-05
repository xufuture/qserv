/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

#ifndef LSST_SG_ORIENTATION_H_
#define LSST_SG_ORIENTATION_H_

/// \file
/// \author Serge Monkewitz
/// \brief This file declares functions for orienting points on the sphere.

#include "UnitVector3d.h"


namespace lsst {
namespace sg {

/// `orientationExact` computes and returns the orientations of 3 vectors a, b
/// and c, which need not be normalized but are assumed to have finite
/// components. The return value is +1 if the vectors a, b, and c are in
/// counter-clockwise orientation, 0 if they are coplanar, colinear, or
/// identical, and -1 if they are in clockwise orientation. The implementation
/// uses arbitrary precision arithmetic to avoid floating point rounding error,
/// underflow and overflow.
int orientationExact(Vector3d const & a,
                     Vector3d const & b,
                     Vector3d const & c);

/// `orientation` computes and returns the orientations of 3 unit vectors a, b
/// and c. The return value is +1 if the vectors a, b, and c are in counter-
/// clockwise orientation, 0 if they are coplanar, colinear or identical, and
/// -1 if they are in clockwise orientation.
///
/// This is equivalent to computing the scalar triple product a · (b x c),
/// which is the sign of the determinant of the 3x3 matrix with a, b
/// and c as columns/rows.
///
/// The implementation proceeds by first computing a double precision
/// approximation, and then falling back to arbitrary precision arithmetic
/// when necessary. Consequently, the result is exact.
inline int orientation(UnitVector3d const & a,
                       UnitVector3d const & b,
                       UnitVector3d const & c)
{
    // This constant is a little more than 5ε, where ε = 2^-53. When multiplied
    // by the permanent of |M|, it gives an error bound on the determinant of
    // M. Here, M is a 3x3 matrix and |M| denotes the matrix obtained by
    // taking the absolute value of each of its components. The derivation of
    // this proceeds in the same manner as the derivation of the error bounds
    // in section 4.3 of:
    //
    //     Adaptive Precision Floating-Point Arithmetic
    //     and Fast Robust Geometric Predicates,
    //     Jonathan Richard Shewchuk,
    //     Discrete & Computational Geometry 18(3):305–363, October 1997.
    //
    // available online at http://www.cs.berkeley.edu/~jrs/papers/robustr.pdf
    static double const relativeError = 5.6e-16;
    // Because all 3 unit vectors are normalized, the maximum absolute value of
    // any vector component, cross product component or dot product term in
    // the calculation is very close to 1. The permanent of |M| must therefore
    // be below 3 + c, where c is some small multiple of ε. This constant, a
    // little larger than 3 * 5ε, is an upper bound on the absolute error in
    // the determinant calculation.
    static double const maxAbsoluteError = 1.7e-15;
    // This constant accounts for floating point underflow (assuming hardware
    // without gradual underflow, just to be conservative) in the computation
    // of det(M). It is a little more than 14 * 2^-1022.
    static double const minAbsoluteError = 4.0e-307;

    double bycz = b.y() * c.z();
    double bzcy = b.z() * c.y();
    double bzcx = b.z() * c.x();
    double bxcz = b.x() * c.z();
    double bxcy = b.x() * c.y();
    double bycx = b.y() * c.x();
    double determinant = a.x() * (bycz - bzcy) +
                         a.y() * (bzcx - bxcz) +
                         a.z() * (bxcy - bycx);
    if (determinant > maxAbsoluteError) {
        return 1;
    } else if (determinant < -maxAbsoluteError) {
        return -1;
    }
    // Expend some more effort on what is hopefully a tighter error bound
    // before falling back on arbitrary precision arithmetic.
    double permanent = std::fabs(a.x()) * (std::fabs(bycz) + std::fabs(bzcy)) +
                       std::fabs(a.y()) * (std::fabs(bzcx) + std::fabs(bxcz)) +
                       std::fabs(a.z()) * (std::fabs(bxcy) + std::fabs(bycx));
    double maxError = relativeError * permanent + minAbsoluteError;
    if (determinant > maxError) {
        return 1;
    } else if (determinant < -maxError) {
        return -1;
    }
    // Avoid the slow path when any two inputs are identical or antipodal.
    if (a == b || b == c || a == c || a == -b || b == -c || a == -c) {
        return 0;
    }
    return orientationExact(a, b, c);
}

}} // namespace lsst::sg

#endif // LSST_SG_ORIENTATION_H_
