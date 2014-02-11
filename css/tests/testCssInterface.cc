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
#include <iostream>
#include <stdexcept>
#include <string.h> // for memset

// boost
#define BOOST_TEST_MODULE MyTest
#include <boost/test/unit_test.hpp>

// local imports
#include "cssInterface.h"

using std::cout;
using std::endl;
using std::string;
using std::vector;

namespace qCss = lsst::qserv::css;


struct CssInterfaceFixture {
    CssInterfaceFixture(void) {
    };
    ~CssInterfaceFixture(void) {
    };
};

BOOST_FIXTURE_TEST_SUITE(CssInterfaceTest, CssInterfaceFixture)

BOOST_AUTO_TEST_CASE(my_test) {
    try {
        string k1 = "/xyzA";
        string k2 = "/xyzB";
        string k3 = "/xyzC";
        string v1 = "firstOne";
        string v2 = "secondOne";

        qCss::CssInterface cssI = qCss::CssInterface("localhost:2181");

        cssI.create(k1, v1);
        cssI.create(k2, v2);

        string s = cssI.get(k1);
        cout << "got " << s << " for key " << k1 << endl;

        cout << "checking exist for " << k1 << endl;
        cout << "got: " << cssI.exists(k1) << endl;

        cout << "checking exist for " << k3 << endl;
        cout << "got: " << cssI.exists(k3) << endl;

        std::vector<string> v = cssI.getChildren("/");
        cout << "children of '/': ";
        for (std::vector<string>::iterator it = v.begin() ; it != v.end(); ++it) {
            cout << *it << " ";
        }
        cout << endl;

        cssI.deleteNode(k1);
        cssI.deleteNode(k2);

        v = cssI.getChildren("/");
        cout << "children of '/': ";
        for (std::vector<string>::iterator it = v.begin() ; it != v.end(); ++it) {
            cout << *it << " ";
        }
        cout << endl;
    } catch (std::runtime_error &ex) {
        cout << "Cought exception: " << ex.what() << endl;
    } 
}

BOOST_AUTO_TEST_SUITE_END()
