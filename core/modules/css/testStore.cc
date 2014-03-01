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
#include <stdexcept>

// boost
#define BOOST_TEST_MODULE TestStore
#include <boost/test/included/unit_test.hpp>
#include <boost/lexical_cast.hpp>

// local imports
#include "Store.h"
#include "cssInterfaceImplZoo.h"
#include "cssException.h"

using std::cout;
using std::endl;
using std::make_pair;
using std::string;
using std::vector;

namespace qCss = lsst::qserv::master;

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
        kv.push_back(make_pair(p + "/Object/partitioning/secIndexColName","objId"));
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

        qCss::CssInterfaceImplZoo cssI = 
            qCss::CssInterfaceImplZoo("localhost:2181", false);
        vector<std::pair<string, string> >::const_iterator itr;
        for (itr=kv.begin() ; itr!=kv.end() ; ++itr) {
            cssI.create(itr->first, itr->second);
        }
        store = new qCss::Store("localhost:2181", prefix);
    };

    ~StoreFixture(void) {
        qCss::CssInterfaceImplZoo cssI = 
            qCss::CssInterfaceImplZoo("localhost:2181", false);
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

BOOST_AUTO_TEST_CASE(checkIfContainsDb) {
    BOOST_REQUIRE( store->checkIfContainsDb("dbA"));
    BOOST_REQUIRE( store->checkIfContainsDb("dbB"));
    BOOST_REQUIRE(!store->checkIfContainsDb("Dummy"));
}

BOOST_AUTO_TEST_CASE(checkIfContainsTable) {
    // it does
    BOOST_REQUIRE(store->checkIfContainsTable("dbA", "Object"));

    // it does not
    BOOST_REQUIRE(!store->checkIfContainsTable("dbA", "NotHere"));

    // for non-existing db
    try {
        store->checkIfContainsTable("Dummy", "NotHere");
    } catch (qCss::CssException& e) {
        BOOST_REQUIRE(e.errCode()==qCss::CssException::DB_DOES_NOT_EXIST);
    }
}

BOOST_AUTO_TEST_CASE(checkIfTableIsChunked) {
    // normal, table exists
    BOOST_REQUIRE( store->checkIfTableIsChunked("dbA", "Object"));
    BOOST_REQUIRE( store->checkIfTableIsChunked("dbA", "Source"));
    BOOST_REQUIRE(!store->checkIfTableIsChunked("dbA", "Exposure"));

    // normal, table does not exist
    try {
        store->checkIfTableIsChunked("dbA", "NotHere");
    } catch (qCss::CssException& e) {
        BOOST_REQUIRE(e.errCode()==qCss::CssException::TB_DOES_NOT_EXIST);
    }

    // for non-existing db
    try {
        store->checkIfTableIsChunked("Dummy", "NotHere");
    } catch (qCss::CssException& e) {
        BOOST_REQUIRE(e.errCode()==qCss::CssException::DB_DOES_NOT_EXIST);
    }
}

BOOST_AUTO_TEST_CASE(checkIfTableIsSubChunked) {
    // normal, table exists
    BOOST_REQUIRE(!store->checkIfTableIsSubChunked("dbA", "Object"));
    BOOST_REQUIRE( store->checkIfTableIsSubChunked("dbA", "Source"));
    BOOST_REQUIRE(!store->checkIfTableIsSubChunked("dbA", "Exposure"));

    // normal, table does not exist
    try {
        store->checkIfTableIsSubChunked("dbA", "NotHere");
    } catch (qCss::CssException& e) {
        BOOST_REQUIRE(e.errCode()==qCss::CssException::TB_DOES_NOT_EXIST);
    }

    // for non-existing db
    try {
        store->checkIfTableIsSubChunked("Dummy", "NotHere");
    } catch (qCss::CssException& e) {
        BOOST_REQUIRE(e.errCode()==qCss::CssException::DB_DOES_NOT_EXIST);
    }
}

BOOST_AUTO_TEST_CASE(getAllowedDbs) {
    vector<string> v = store->getAllowedDbs();
    BOOST_REQUIRE(3 == v.size());
    std::sort (v.begin(), v.end());
    BOOST_REQUIRE(v[0]=="dbA");
    BOOST_REQUIRE(v[1]=="dbB");
    BOOST_REQUIRE(v[2]=="dbC");
}

BOOST_AUTO_TEST_CASE(getChunkedTables) {
    // normal, 3 values
    vector<string> v = store->getChunkedTables("dbA");
    BOOST_REQUIRE(3 == v.size());
    std::sort (v.begin(), v.end());
    BOOST_REQUIRE(v[0]=="FSource");
    BOOST_REQUIRE(v[1]=="Object");
    BOOST_REQUIRE(v[2]=="Source");

    // normal, no values
    v = store->getChunkedTables("dbB");
    BOOST_REQUIRE(0 == v.size());

    // for non-existing db
    try {
        store->getChunkedTables("Dummy");
    } catch (qCss::CssException& e) {
        BOOST_REQUIRE(e.errCode()==qCss::CssException::DB_DOES_NOT_EXIST);
    }
}

BOOST_AUTO_TEST_CASE(getSubChunkedTables) {
    // normal, 2 values
    vector<string> v = store->getSubChunkedTables("dbA");
    BOOST_REQUIRE(2 == v.size());
    std::sort (v.begin(), v.end());
    BOOST_REQUIRE(v[0]=="FSource");
    BOOST_REQUIRE(v[1]=="Source");

    // normal, no values
    v = store->getSubChunkedTables("dbB");
    BOOST_REQUIRE(0 == v.size());

    // for non-existing db
    try {
        store->getSubChunkedTables("Dummy");
    } catch (qCss::CssException& e) {
        BOOST_REQUIRE(e.errCode()==qCss::CssException::DB_DOES_NOT_EXIST);
    }
}

BOOST_AUTO_TEST_CASE(getPartitionCols) {
    // normal, has value
    vector<string> v = store->getPartitionCols("dbA", "Object");
    BOOST_REQUIRE(v.size() == 3);
    BOOST_REQUIRE(v[0] == "ra_PS");
    BOOST_REQUIRE(v[1] == "decl_PS");
    BOOST_REQUIRE(v[2] == "objId");

    v = store->getPartitionCols("dbA", "Source");
    BOOST_REQUIRE(v.size() == 3);
    BOOST_REQUIRE(v[0] == "ra");
    BOOST_REQUIRE(v[1] == "decl");
    BOOST_REQUIRE(v[2] == "");

    // for non-existing db
    try {
        store->getPartitionCols("Dummy", "x");
    } catch (qCss::CssException& e) {
        BOOST_REQUIRE(e.errCode()==qCss::CssException::DB_DOES_NOT_EXIST);
    }
}

BOOST_AUTO_TEST_CASE(getChunkLevel) {
    // FIXME
}

BOOST_AUTO_TEST_CASE(getKeyColumn) {
    // normal, has value
    BOOST_REQUIRE(store->getKeyColumn("dbA", "Object") == "objId");

    // normal, does not have value
    BOOST_REQUIRE(store->getKeyColumn("dbA", "Source") == "");

    // for non-existing db
    try {
        store->getKeyColumn("Dummy", "x");
    } catch (qCss::CssException& e) {
        BOOST_REQUIRE(e.errCode()==qCss::CssException::DB_DOES_NOT_EXIST);
    }
}

BOOST_AUTO_TEST_SUITE_END()
