/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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
#define BOOST_TEST_MODULE Path_1
#include "boost/test/included/unit_test.hpp"
// #include <cerrno>
// #include <cstdlib>
// #include <cstring>
#include <iostream>
#include "boost/scoped_ptr.hpp"
#include "QservPath.hh"

namespace test = boost::test_tools;
namespace qsrv = lsst::qserv;

struct PathFixture {
    PathFixture(void) {}
    ~PathFixture(void) { };
};

BOOST_FIXTURE_TEST_SUITE(PathTestSuite, PathFixture)

BOOST_AUTO_TEST_CASE(QueryPathCreate) {
    qsrv::QservPath qp;
    qp.setAsCquery("LSST", 3141);
    BOOST_CHECK_EQUAL(qp.path(), "/q/LSST/3141");
}

BOOST_AUTO_TEST_CASE(QueryPathRead) {
    std::string testPath("/q/DC4/32767");
    qsrv::QservPath qp(testPath);
    BOOST_CHECK_EQUAL(qp.requestType(), qsrv::QservPath::CQUERY);
    BOOST_CHECK_EQUAL(qp.db(), "DC4");
    BOOST_CHECK_EQUAL(qp.chunk(), 32767);
    BOOST_CHECK_EQUAL(qp.path(), testPath);
}

BOOST_AUTO_TEST_CASE(QueryPathRead2) {
    std::string testPath("/q/LSST/185");
    qsrv::QservPath qp(testPath);
    BOOST_CHECK_EQUAL(qp.requestType(), qsrv::QservPath::CQUERY);
    BOOST_CHECK_EQUAL(qp.db(), "LSST");
    BOOST_CHECK_EQUAL(qp.chunk(), 185);
    BOOST_CHECK_EQUAL(qp.path(), testPath);
}


BOOST_AUTO_TEST_CASE(QueryPathOld) {
    std::string testPath1("/query/32767");
    qsrv::QservPath qp1(testPath1);
    BOOST_CHECK_EQUAL(qp1.requestType(), qsrv::QservPath::OLDQ1);
    BOOST_CHECK_EQUAL(qp1.chunk(), 32767); 

    std::string testPath2("/query2/32767");
    qsrv::QservPath qp2(testPath2);
    BOOST_CHECK_EQUAL(qp2.requestType(), qsrv::QservPath::OLDQ2);
    BOOST_CHECK_EQUAL(qp2.chunk(), 32767);
}

BOOST_AUTO_TEST_CASE(PathWithKeys) {
    std::string testPath1("/result/1234567890abcdef?debug&fun=yes&obj=world");
    qsrv::QservPath qp1(testPath1);
    BOOST_CHECK_EQUAL(qp1.requestType(), qsrv::QservPath::RESULT);
    BOOST_CHECK_EQUAL(qp1.hashName(), "1234567890abcdef");
    BOOST_CHECK_EQUAL(qp1.path(), testPath1);
}

BOOST_AUTO_TEST_CASE(CreateKeyPath) {
    std::string test1("/result/abcdef1234567890?batch&bsize=5&session=test");
    qsrv::QservPath qp1;
    std::string hName("abcdef1234567890");
    qp1.setAsResult(hName);
    qp1.addKey("batch");
    qp1.addKey("bsize", 5);
    qp1.addKey("session", "test");
    BOOST_CHECK_EQUAL(qp1.requestType(), qsrv::QservPath::RESULT);
    BOOST_CHECK_EQUAL(qp1.hashName(), hName);
    BOOST_CHECK_EQUAL(qp1.path(), test1);
}


BOOST_AUTO_TEST_SUITE_END()
