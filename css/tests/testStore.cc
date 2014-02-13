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
  * @file testStore.cc
  *
  * @brief Unit test for the Store.
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
#include <boost/test/unit_test.hpp>
#include <boost/lexical_cast.hpp>

// local imports
#include "Store.h"
#include "cssException.h"

using std::cout;
using std::endl;
using std::make_pair;
using std::ostringstream;
using std::string;
using std::vector;

namespace qCss = lsst::qserv::css;


struct StoreFixture {
    StoreFixture(void) {
        string prefix = "/unittest_" + boost::lexical_cast<string>(rand());
        cout << "My prefix is: " << prefix << endl;
        kv.push_back(make_pair(prefix, ""));
        kv.push_back(make_pair(prefix + "/DATABASES", ""));

        kv.push_back(make_pair(prefix + "/DATABASES/dbA", ""));
        kv.push_back(make_pair(prefix + "/DATABASES/dbB", ""));
        kv.push_back(make_pair(prefix + "/DATABASES/dbC", ""));
        string p = prefix + "/DATABASES/dbA/TABLES";
        kv.push_back(make_pair(p, ""));
        kv.push_back(make_pair(p + "/Object", ""));
        kv.push_back(make_pair(p + "/Object/partitioning", ""));
        kv.push_back(make_pair(p + "/Object/partitioning/lonColName", "ra_PS"));
        kv.push_back(make_pair(p + "/Object/partitioning/latColName", "decl_PS"));
        kv.push_back(make_pair(p + "/Object/partitioning/keyColName", "objId"));
        kv.push_back(make_pair(p + "/Source", ""));
        kv.push_back(make_pair(p + "/Source/partitioning", ""));
        kv.push_back(make_pair(p + "/Source/partitioning/lonColName", "ra"));
        kv.push_back(make_pair(p + "/Source/partitioning/latColName", "decl"));
        kv.push_back(make_pair(p + "/Source/partitioning/subChunks", "1"));
        kv.push_back(make_pair(p + "/FSource", ""));
        kv.push_back(make_pair(p + "/FSource/partitioning", ""));
        kv.push_back(make_pair(p + "/FSource/partitioning/lonColName", "ra"));
        kv.push_back(make_pair(p + "/FSource/partitioning/latColName", "decl"));
        kv.push_back(make_pair(p + "/FSource/partitioning/subChunks", "1"));
        kv.push_back(make_pair(p + "/Exposure", ""));

        p = prefix + "/DATABASES/dbB/TABLES";
        kv.push_back(make_pair(p, ""));
        kv.push_back(make_pair(p + "/Exposure", ""));

        qCss::CssInterface cssI = qCss::CssInterface("localhost:2181");
        vector<std::pair<string, string> >::const_iterator itr;
        for (itr=kv.begin() ; itr!=kv.end() ; ++itr) {
            cssI.create(itr->first, itr->second);
        }
        store = new qCss::Store("localhost:2181", prefix);
    };

    ~StoreFixture(void) {
        qCss::CssInterface cssI = qCss::CssInterface("localhost:2181");
        vector<std::pair<string, string> >::const_reverse_iterator itr;
        for (itr=kv.rbegin() ; itr!=kv.rend() ; ++itr) {
            cssI.deleteNode(itr->first);
        }
        delete store;
    };

    vector<std::pair<string, string> > kv;
    qCss::Store *store;
};

BOOST_FIXTURE_TEST_SUITE(StoreTest, StoreFixture)

BOOST_AUTO_TEST_CASE(testDbs) {
    BOOST_REQUIRE( store->checkIfContainsDb("dbA"));
    BOOST_REQUIRE( store->checkIfContainsDb("dbB"));
    BOOST_REQUIRE(!store->checkIfContainsDb("Dummy"));

    vector<string> v = store->getAllowedDbs();
    BOOST_REQUIRE(3 == v.size());
    std::sort (v.begin(), v.end());
    BOOST_REQUIRE(v[0]=="dbA");
    BOOST_REQUIRE(v[1]=="dbB");
    BOOST_REQUIRE(v[2]=="dbC");
}

BOOST_AUTO_TEST_CASE(checkTables) {
    BOOST_REQUIRE( store->checkIfContainsTable("dbA", "Object"));
    BOOST_REQUIRE(!store->checkIfContainsTable("dbA", "NotHere"));

    BOOST_REQUIRE( store->checkIfTableIsChunked("dbA", "Object"));
    BOOST_REQUIRE( store->checkIfTableIsChunked("dbA", "Source"));
    BOOST_REQUIRE(!store->checkIfTableIsChunked("dbA", "Exposure"));
    BOOST_REQUIRE(!store->checkIfTableIsChunked("dbA", "NotHere"));

    vector<string> v = store->getChunkedTables("dbA");
    BOOST_REQUIRE(3 == v.size());
    std::sort (v.begin(), v.end());
    BOOST_REQUIRE(v[0]=="FSource");
    BOOST_REQUIRE(v[1]=="Object");
    BOOST_REQUIRE(v[2]=="Source");

    v = store->getChunkedTables("dbB");
    BOOST_REQUIRE(0 == v.size());

    store->getKeyColumn("dbA", "Object");
    try {
        store->getKeyColumn("dbA", "Source");
    } catch (qCss::CssException& e) {
        BOOST_REQUIRE(e.errCode()==qCss::CssException::KEY_DOES_NOT_EXIST);
    }
}

BOOST_AUTO_TEST_SUITE_END()
