// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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
  * @file
  *
  * @brief Unit test for the MySql implementation of the Common State System Interface.
  *
  * @Author Nathan Pease, SLAC
  */

// System headers
#include <iostream>
#include <string>
#include <unistd.h>

// Boost headers
#include <boost/algorithm/string/replace.hpp>

// Boost unit test header
#define BOOST_TEST_MODULE Css_1
#include "boost/test/included/unit_test.hpp"

// Qserv headers
#include "css/CssError.h"
#include "css/KvInterfaceImplMysql.h"
#include "mysql/MySqlConfig.h"
#include "sql/SqlConnection.h"
#include "sql/SqlErrorObject.h"

using lsst::qserv::css::KvInterfaceImplMySql;
using lsst::qserv::css::SqlError;
using lsst::qserv::mysql::MySqlConfig;
using lsst::qserv::sql::SqlConnection;
using lsst::qserv::sql::SqlErrorObject;

namespace {

// todo this is copied directly from testQMeta.cc. Perhaps it could be made generic and put in a shared location?
struct TestDBGuard {
    TestDBGuard() {
        sqlConfig.hostname = "";
        sqlConfig.port = 0;
        sqlConfig.username = "root";
        sqlConfig.password = "changeme"; //getpass("Enter mysql root password: ");
//        std::cout << "Enter mysql socket: ";
//        std::cin >> sqlConfig.socket;
        sqlConfig.socket = "/home/n8pease/qserv-run/2015_07/var/lib/mysql/mysql.sock";

        //create a db name with username & random number
        sqlConfig.dbName = "testCSSZ012sdrt"; // (why Z012sdrt? what does it mean?)

        std::ifstream schemaFile("admin/templates/configuration/tmp/configure/sql/CssData.sql");

        // read whole file into buffer
        std::string buffer;
        std::getline(schemaFile, buffer, '\0');

        // replace production schema name with test schema
        boost::replace_all(buffer, "qservCssData", sqlConfig.dbName);

        // need config without database name
        MySqlConfig sqlConfigLocal = sqlConfig;
        sqlConfigLocal.dbName = "";
        SqlConnection sqlConn(sqlConfigLocal);

        SqlErrorObject errObj;
        sqlConn.runQuery(buffer, errObj);
        if (errObj.isSet()) {
            throw SqlError(errObj);
        }
    }

    ~TestDBGuard() {
        SqlConnection sqlConn(sqlConfig);
        SqlErrorObject errObj;
        sqlConn.dropDb(sqlConfig.dbName, errObj);
    }

    MySqlConfig sqlConfig;
};

} // namespace

struct PerTestFixture
{
    PerTestFixture() {
        kvInterface = std::make_shared<KvInterfaceImplMySql>(testDB.sqlConfig);
        sqlConn = std::make_shared<SqlConnection>(testDB.sqlConfig);
    }

    static TestDBGuard testDB;
    std::shared_ptr<SqlConnection> sqlConn;
    std::shared_ptr<KvInterfaceImplMySql> kvInterface;
};

TestDBGuard PerTestFixture::testDB;


BOOST_FIXTURE_TEST_SUITE(SqlKVInterfaceConnectionTestSuite, PerTestFixture)


BOOST_AUTO_TEST_CASE(CreateAndGetKV) {
    kvInterface->create("/foo/testKey", "testValue");
    BOOST_CHECK_EQUAL(kvInterface->get("/foo/testKey"), "testValue");
}


BOOST_AUTO_TEST_CASE(CreateWithBackslash) {
    kvInterface->create("\\foo\\testKey", "testValue");
    BOOST_CHECK_EQUAL(kvInterface->get("\\foo\\testKey"), "testValue");
}


BOOST_AUTO_TEST_CASE(SetAndGetChildren) {
    kvInterface->create("setAndGetChildren/child0", "abc");
    kvInterface->create("setAndGetChildren/child1", "abc");
    kvInterface->create("setAndGetChildren/child2", "abc");
    std::vector<std::string> children = kvInterface->getChildren("setAndGetChildren");
    BOOST_REQUIRE(children.size() == 3);
    std::sort(children.begin(), children.end());
    BOOST_CHECK_EQUAL(children[0], "setAndGetChildren/child0");
    BOOST_CHECK_EQUAL(children[1], "setAndGetChildren/child1");
    BOOST_CHECK_EQUAL(children[2], "setAndGetChildren/child2");
}


BOOST_AUTO_TEST_CASE(CreateDuplicateKV) {
    BOOST_REQUIRE_NO_THROW(kvInterface->create("duplicateKey", "a value"));
    // verify that adding a key a second time does not throw
    BOOST_CHECK_THROW(kvInterface->create("duplicateKey", "another value"), lsst::qserv::css::KeyExistsError);
}


BOOST_AUTO_TEST_CASE(Exists) {
    // new key, should not exist:
    BOOST_REQUIRE_EQUAL(kvInterface->exists("non existent key"), false);
    // adding the key should work:
    BOOST_REQUIRE_NO_THROW(kvInterface->create("non existent key", "new value"));
    // now the key should exist:
    BOOST_REQUIRE_EQUAL(kvInterface->exists("non existent key"), true);
}


BOOST_AUTO_TEST_CASE(Delete) {
    BOOST_REQUIRE_NO_THROW(kvInterface->create("KeyToDelete", "a value"));
    BOOST_REQUIRE_NO_THROW(kvInterface->deleteKey("KeyToDelete"));
    BOOST_REQUIRE_THROW(kvInterface->deleteKey("KeyToDelete"), lsst::qserv::css::NoSuchKey);
}


BOOST_AUTO_TEST_CASE(Set) {
    // call set with a key that does not exist (it should get added)
    BOOST_REQUIRE_NO_THROW(kvInterface->set("SetNonExistentKey", "nowItExists"));
    // verify the key was added:
    BOOST_CHECK_EQUAL(kvInterface->get("SetNonExistentKey"), "nowItExists");
    // set the key to a new value
    BOOST_REQUIRE_NO_THROW(kvInterface->set("SetNonExistentKey", "toANewValue"));
    // verify the change:
    BOOST_CHECK_EQUAL(kvInterface->get("SetNonExistentKey"), "toANewValue");
}


BOOST_AUTO_TEST_SUITE_END()
