/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
 
#define BOOST_TEST_MODULE MySqlFsFile_1
#include "boost/test/included/unit_test.hpp"
#include "lsst/qserv/QservPath.hh"

namespace test = boost::test_tools;
namespace qsrv = lsst::qserv;

struct Fixture {
    Fixture() {}
};


BOOST_FIXTURE_TEST_SUITE(FsFile, Fixture)

BOOST_AUTO_TEST_CASE(TestParse) {
    std::string prefix("/result/");
    std::string hash("1122334455");
    qsrv::QservPath qp( prefix + hash + "?obj=earth");
    BOOST_CHECK_EQUAL(qp.requestType(), qsrv::QservPath::RESULT);
    BOOST_CHECK_EQUAL(qp.hashName(), hash);
    BOOST_CHECK_EQUAL(qp.var("obj"), "earth");
}

BOOST_AUTO_TEST_CASE(TestParse2) {
    std::string prefix("/result/");
    std::string hash("abcde12345");
    qsrv::QservPath qp( prefix + hash + "?obj=world&fun=yes&debug&obj=earth");
    BOOST_CHECK_EQUAL(qp.requestType(), qsrv::QservPath::RESULT);
    BOOST_CHECK_EQUAL(qp.hashName(), hash);
    BOOST_CHECK_EQUAL(qp.var("obj"), "earth");
    BOOST_CHECK_EQUAL(qp.var("fun"), "yes");
    BOOST_CHECK(qp.hasVar("debug"));
    BOOST_CHECK_EQUAL(qp.var("debug"), std::string());
}

BOOST_AUTO_TEST_CASE(FileBuild) {
    qsrv::QservPath p;
    std::string fstr("1234567890abcdef");
    p.setAsResult(fstr);
    p.addVar("obj", "world");
    p.addVar("batch","yes");
    BOOST_CHECK_EQUAL(p.hashName(), fstr);
    BOOST_CHECK(p.hasVar("obj"));
    BOOST_CHECK(p.hasVar("batch"));
    BOOST_CHECK_EQUAL(p.requestType(), qsrv::QservPath::RESULT);

    qsrv::QservPath p2(p.path());
    BOOST_CHECK_EQUAL(p2.hashName(), fstr);
    BOOST_CHECK(p2.hasVar("obj"));
    BOOST_CHECK(p2.hasVar("batch"));
    BOOST_CHECK_EQUAL(p2.requestType(), qsrv::QservPath::RESULT);
}

BOOST_AUTO_TEST_SUITE_END()
