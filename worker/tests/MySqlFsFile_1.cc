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
#include "lsst/qserv/worker/XrootFilename.h"

namespace test = boost::test_tools;
namespace qWorker = lsst::qserv::worker;

struct Fixture {
    Fixture() {}
};


BOOST_FIXTURE_TEST_SUITE(FsFile, Fixture)

BOOST_AUTO_TEST_CASE(TestParse) {
    qWorker::XrootFilename fn("hello?obj=world&fun=yes&debug");
    BOOST_CHECK_EQUAL(fn.getFile(), "hello");
    BOOST_CHECK_EQUAL(fn.getQueryString(), "obj=world&fun=yes&debug");
    BOOST_CHECK(fn.hasKey("obj"));
    BOOST_CHECK(fn.hasKey("fun"));
    BOOST_CHECK(fn.hasKey("debug"));
    BOOST_CHECK_EQUAL(fn.getValue("obj"), "world");
}

BOOST_AUTO_TEST_CASE(TestParse2) {
    qWorker::XrootFilename fn("hello?obj=world&fun=yes&debug&obj=earth");
    BOOST_CHECK_EQUAL(fn.getFile(), "hello");
    BOOST_CHECK_EQUAL(fn.getQueryString(), "obj=world&fun=yes&debug&obj=earth");
    BOOST_CHECK(fn.hasKey("obj"));
    BOOST_CHECK(fn.hasKey("fun"));
    BOOST_CHECK(fn.hasKey("debug"));
    BOOST_CHECK_EQUAL(fn.getValue("obj"), "earth");
}

BOOST_AUTO_TEST_CASE(FileBuild) {
    std::string fstr("1234567890abcdef");
    qWorker::XrootFilename fn(fstr);
    fn.addValue("obj", "world");
    fn.addValue("batch","yes");
    BOOST_CHECK_EQUAL(fn.getFile(), fstr);
    BOOST_CHECK(fn.hasKey("obj"));
    BOOST_CHECK(fn.hasKey("batch"));
    BOOST_CHECK_EQUAL(fn.getValue("obj"), "world");
    BOOST_CHECK_EQUAL(fn.getValue("batch"), "yes");
    BOOST_CHECK_EQUAL(fn.getQueryString(), "batch=yes&obj=world");
}

BOOST_AUTO_TEST_SUITE_END()
