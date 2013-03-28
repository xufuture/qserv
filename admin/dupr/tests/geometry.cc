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

#include <stdexcept>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE Vector
#include "boost/test/unit_test.hpp"

#include "Constants.h"
#include "Geometry.h"

using std::cos;
using std::exception;
using std::fabs;
using std::pair;
using std::sin;
using std::sqrt;
using lsst::qserv::admin::dupr::Vector3d;

namespace dupr = lsst::qserv::admin::dupr;


namespace {

    void checkClose(Vector3d const & u, Vector3d const & v, double fraction) {
        BOOST_CHECK_CLOSE_FRACTION(u.dot(v), u.norm()*v.norm(), fraction);
    }

    void checkClose(pair<double, double> const & u,
                    pair<double, double> const & v, double fraction) {
        BOOST_CHECK_CLOSE_FRACTION(u.first, v.first, fraction);
        BOOST_CHECK_CLOSE_FRACTION(u.second, v.second, fraction);
    }

    void northEast(Vector3d & n, Vector3d & e, Vector3d const & v) {
        n(0) = -v(0)*v(2);
        n(1) = -v(1)*v(2);
        n(2) = v(0)*v(0) + v(1)*v(1);
        if (n(0) == 0.0 && n(1) == 0.0 && n(2) == 0.0) {
           n(0) = -1.0;
           e(0) = 0.0;
           e(1) = 1.0;
           e(2) = 0.0;
        } else {
           n = n.normalized();
           e = n.cross(v).normalized();
        }
    }

    enum {
        S0 = (0 + 8), S00 = (0 + 8)*4, S01, S02, S03,
        S1 = (1 + 8), S10 = (1 + 8)*4, S11, S12, S13,
        S2 = (2 + 8), S20 = (2 + 8)*4, S21, S22, S23,
        S3 = (3 + 8), S30 = (3 + 8)*4, S31, S32, S33,
        N0 = (4 + 8), N00 = (4 + 8)*4, N01, N02, N03,
        N1 = (5 + 8), N10 = (5 + 8)*4, N11, N12, N13,
        N2 = (6 + 8), N20 = (6 + 8)*4, N21, N22, N23,
        N3 = (7 + 8), N30 = (7 + 8)*4, N31, N32, N33
    };

    size_t const NPOINTS = 38;
    double const C0 = 0.577350269189625764509148780503; // √3/3
    double const C1 = 0.270598050073098492199861602684; // 1 / (2*√(2 + √2))
    double const C2 = 0.923879532511286756128183189400; // (1 + √2) / (√2 * √(2 + √2))

    Vector3d const _points[NPOINTS] = {
        Vector3d(  1,  0,  0), //  x
        Vector3d(  0,  1,  0), //  y
        Vector3d(  0,  0,  1), //  z
        Vector3d( -1,  0,  0), // -x
        Vector3d(  0, -1,  0), // -y
        Vector3d(  0,  0, -1), // -z
        Vector3d( C0, C0, C0), // center of N3
        Vector3d(-C0, C0, C0), // center of N2
        Vector3d(-C0,-C0, C0), // center of N1
        Vector3d( C0,-C0, C0), // center of N0
        Vector3d( C0, C0,-C0), // center of S0
        Vector3d(-C0, C0,-C0), // center of S1
        Vector3d(-C0,-C0,-C0), // center of S2
        Vector3d( C0,-C0,-C0), // center of S3
        Vector3d( C1, C1, C2), // center of N31
        Vector3d( C2, C1, C1), // center of N32
        Vector3d( C1, C2, C1), // center of N30
        Vector3d(-C1, C1, C2), // center of N21
        Vector3d(-C1, C2, C1), // center of N22
        Vector3d(-C2, C1, C1), // center of N20
        Vector3d(-C1,-C1, C2), // center of N11
        Vector3d(-C2,-C1, C1), // center of N12
        Vector3d(-C1,-C2, C1), // center of N10
        Vector3d( C1,-C1, C2), // center of N01
        Vector3d( C1,-C2, C1), // center of N02
        Vector3d( C2,-C1, C1), // center of N00
        Vector3d( C1, C1,-C2), // center of S01
        Vector3d( C2, C1,-C1), // center of S00
        Vector3d( C1, C2,-C1), // center of S02
        Vector3d(-C1, C1,-C2), // center of S11
        Vector3d(-C1, C2,-C1), // center of S10
        Vector3d(-C2, C1,-C1), // center of S12
        Vector3d(-C1,-C1,-C2), // center of S21
        Vector3d(-C2,-C1,-C1), // center of S20
        Vector3d(-C1,-C2,-C1), // center of S22
        Vector3d( C1,-C1,-C2), // center of S31
        Vector3d( C1,-C2,-C1), // center of S30
        Vector3d( C2,-C1,-C1), // center of S32
    };

    uint32_t const _ids[NPOINTS] = {
        N32, N22, N31, N12, N02, S01,
        N33, N23, N13, N03, S03, S13, S23, S33,
        N31, N32, N30, N21, N22, N20, N11, N12,
        N10, N01, N02, N00, S01, S00, S02, S11,
        S10, S12, S21, S20, S22, S31, S30, S32
    };
} // unnamed namespace


BOOST_AUTO_TEST_CASE(clampDec) {
    BOOST_CHECK_EQUAL(dupr::clampDec(-91.0), -90.0);
    BOOST_CHECK_EQUAL(dupr::clampDec( 91.0),  90.0);
    BOOST_CHECK_EQUAL(dupr::clampDec( 89.0),  89.0);
}

BOOST_AUTO_TEST_CASE(minDeltaRa) {
    BOOST_CHECK_EQUAL(dupr::minDeltaRa(1.0, 2.0), 1.0);
    BOOST_CHECK_EQUAL(dupr::minDeltaRa(359.0, 1.0), 2.0);
    BOOST_CHECK_EQUAL(dupr::minDeltaRa(10.0, 350.0), 20.0);
}

BOOST_AUTO_TEST_CASE(reduceRa) {
    BOOST_CHECK_EQUAL(dupr::reduceRa(0.0), 0.0);
    BOOST_CHECK_EQUAL(dupr::reduceRa(360.0), 0.0);
    BOOST_CHECK_EQUAL(dupr::reduceRa(540.0), 180.0);
    BOOST_CHECK_EQUAL(dupr::reduceRa(-180.0), 180.0);
}

BOOST_AUTO_TEST_CASE(maxAlpha) {
    // Check corner cases.
    BOOST_CHECK_EQUAL(dupr::maxAlpha(10.0, 85.0), 180.0);
    BOOST_CHECK_EQUAL(dupr::maxAlpha(10.0, -85.0), 180.0);
    BOOST_CHECK_EQUAL(dupr::maxAlpha(0.0, 30.0), 0.0);
    BOOST_CHECK_THROW(dupr::maxAlpha(-1.0, 0.0), exception);
    BOOST_CHECK_THROW(dupr::maxAlpha(91.0, 0.0), exception);
    // Generate points in a circle of radius 1 deg and check
    // that each point has RA within alpha of the center RA.
    double const dec = 45.0;
    double const r = 1.0;
    double const alpha = dupr::maxAlpha(r, dec);
    Vector3d n, e, v = dupr::cartesian(0.0, dec);
    northEast(n, e, v);
    double sinr = sin(r * dupr::RAD_PER_DEG);
    double cosr = cos(r * dupr::RAD_PER_DEG);
    for (double a = 0; a < 360.0; a += 0.0625) {
        double sina = sin(a * dupr::RAD_PER_DEG);
        double cosa = cos(a * dupr::RAD_PER_DEG);
        Vector3d p = cosr * v + sinr * (cosa * n + sina * e);
        double ra = dupr::minDeltaRa(0.0, dupr::spherical(p).first);
        BOOST_CHECK_LT(ra, alpha + dupr::EPSILON_DEG);
    }
}

BOOST_AUTO_TEST_CASE(htmId) {
    // Check corner cases.
    Vector3d x(1,0,0);
    BOOST_CHECK_THROW(dupr::htmId(x, -1), exception);
    BOOST_CHECK_THROW(dupr::htmId(x, dupr::HTM_MAX_LEVEL + 1), exception);
    // Check test points.
    for (size_t i = 0; i < NPOINTS; ++i) {
        uint32_t h = _ids[i];
        BOOST_CHECK_EQUAL(dupr::htmId(_points[i], 1), h);
        BOOST_CHECK_EQUAL(dupr::htmId(_points[i], 0), (h >> 2));
    }
}

BOOST_AUTO_TEST_CASE(htmLevel) {
    for (uint32_t i = 0; i < 8; ++i) {
        BOOST_CHECK_EQUAL(dupr::htmLevel(i), -1);
    }
    for (uint32_t i = 8; i < 16; ++i) {
        BOOST_CHECK_EQUAL(dupr::htmLevel(i), 0);
    }
    BOOST_CHECK_EQUAL(dupr::htmLevel(0x80), 2);
    for (int l = 0; l <= dupr::HTM_MAX_LEVEL; ++l) {
        BOOST_CHECK_EQUAL(dupr::htmLevel(0x8 << (2*l)), l);
        BOOST_CHECK_EQUAL(dupr::htmLevel(0x8 << (2*l + 1)), -1);
    }
}

BOOST_AUTO_TEST_CASE(cartesian) {
    double const f = 1e-15;
    checkClose(dupr::cartesian( 90,  0), Vector3d( 0, 1, 0), f);
    checkClose(dupr::cartesian(180,  0), Vector3d(-1, 0, 0), f);
    checkClose(dupr::cartesian( 55, 90), Vector3d( 0, 0, 1), f);
    checkClose(dupr::cartesian(999,-90), Vector3d( 0, 0,-1), f);
    checkClose(dupr::cartesian( 45,  0)*2, Vector3d(sqrt(2.), sqrt(2.), 0), f);
    checkClose(dupr::cartesian( 45, 45)*2, Vector3d(1, 1, sqrt(2.)), f);
}

BOOST_AUTO_TEST_CASE(spherical) {
    checkClose(pair<double, double>(45, 45),
               dupr::spherical(1, 1, sqrt(2.)), 1e-15);
    checkClose(pair<double, double>(45, -45),
               dupr::spherical(1, 1, -sqrt(2.)), 1e-15);
}
