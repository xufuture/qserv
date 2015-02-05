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

/// \file
/// \author Serge Monkewitz
/// \brief This file contains the Vector3d class implementation.

#include "Vector3d.h"

#include <cstdio>
#include <ostream>

#include "Angle.h"
#include "UnitVector3d.h"


namespace lsst {
namespace sg {

Vector3d Vector3d::rotatedAround(UnitVector3d const & k, Angle a) const {
    // Use Rodrigues' rotation formula.
    Vector3d const & v = *this;
    double s = sin(a);
    double c = cos(a);
    return v * c + k.cross(v) * s + k * (k.dot(v) * (1.0 - c));
}

std::ostream & operator<<(std::ostream & os, Vector3d const & v) {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "Vector3d(%.17g, %.17g, %.17g)",
                  v.x(), v.y(), v.z());
    return os << buf;
}

}} // namespace lsst::sg
