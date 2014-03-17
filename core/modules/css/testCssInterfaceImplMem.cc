// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

/**
  * @file testCssInterface.cc
  *
  * @brief Unit test for the Common State System Interface.
  *
  * @Author Jacek Becla, SLAC
  */


// standard library imports
#include <algorithm> // sort
#include <cstdlib>   // rand
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string.h>  // memset

// boost
#define BOOST_TEST_MODULE MyTest
#include <boost/test/included/unit_test.hpp>
#include <boost/lexical_cast.hpp>

// local imports
#include "cssInterfaceImplMem.h"

using std::cout;
using std::endl;
using std::ostringstream;
using std::string;
using std::vector;

struct CssInterfaceFixture {
    CssInterfaceFixture(void) {
        prefix = "/unittest_" + boost::lexical_cast<std::string>(rand());
        k1 = prefix + "/xyzA";
        k2 = prefix + "/xyzB";
        k3 = prefix + "/xyzC";
        v1 = "firstOne";
        v2 = "secondOne";
    };
    ~CssInterfaceFixture(void) {
    };
    string prefix, k1, k2, k3, v1, v2;
};

BOOST_FIXTURE_TEST_SUITE(CssInterfaceTest, CssInterfaceFixture)

BOOST_AUTO_TEST_CASE(createGetCheck) {
    lsst::qserv::css::CssInterfaceImplMem cssI =
        lsst::qserv::css::CssInterfaceImplMem(true);

    cssI.create(prefix, v1);
    cssI.create(k1, v1);
    cssI.create(k2, v2);
    string s = cssI.get(k1);
    BOOST_CHECK(s == v1);
    BOOST_CHECK(cssI.exists(k1));
    BOOST_CHECK(!cssI.exists(k3));

    vector<string> v = cssI.getChildren(prefix);
    BOOST_CHECK(2 == v.size());
    std::sort (v.begin(), v.end());
    BOOST_CHECK(v[0]=="xyzA");
    BOOST_CHECK(v[1]=="xyzB");

    cssI.deleteNode(k1);

    v = cssI.getChildren(prefix);
    BOOST_CHECK(1 == v.size());

    cssI.deleteNode(k2);
    cssI.deleteNode(prefix);
}

BOOST_AUTO_TEST_SUITE_END()
