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
  * @brief Simple testing for class QueryAction
  * Requires some setup, and assumes some access to a mysqld
  *
  * @author Daniel L. Wang, SLAC
  */

// Local headers
#include "proto/worker.pb.h"
#include "wbase/SendChannel.h"
#include "wbase/Task.h"
#include "wdb/QueryAction.h"
#include "wlog/WLogger.h"

// Boost unit test header
#define BOOST_TEST_MODULE QueryAction
#include "boost/test/included/unit_test.hpp"

namespace test = boost::test_tools;

using lsst::qserv::proto::TaskMsg;
using lsst::qserv::proto::TaskMsg_Subchunk;
using lsst::qserv::proto::TaskMsg_Fragment;

using lsst::qserv::wbase::SendChannel;
using lsst::qserv::wdb::QueryAction;
using lsst::qserv::wdb::QueryActionArg;
using lsst::qserv::wlog::WLogger;

struct Fixture {
    boost::shared_ptr<TaskMsg> newTaskMsg() {
        boost::shared_ptr<TaskMsg> t(new TaskMsg);
        t->set_protocol(2);
        t->set_session(123456);
        t->set_chunkid(3240); // hardcoded
        t->set_db("LSST"); // hardcoded
        t->add_scantables("Object");
        lsst::qserv::proto::TaskMsg::Fragment* f = t->add_fragment();
        f->add_query("SELECT AVG(yFlux_PS) from LSST.Object_3240");
        f->set_resulttable("r_341");
        return t;
    }
    QueryActionArg newArg() {
        boost::shared_ptr<TaskMsg> msg(newTaskMsg());
        boost::shared_ptr<SendChannel> sc(SendChannel::newNopChannel());
        lsst::qserv::wbase::Task::Ptr t(new lsst::qserv::wbase::Task(msg, sc));
        boost::shared_ptr<WLogger> w(new WLogger(WLogger::Printer::newCout()));
        QueryActionArg a(w, t);
        return a;
    }
};


BOOST_FIXTURE_TEST_SUITE(Basic, Fixture)

BOOST_AUTO_TEST_CASE(Simple) {

    QueryActionArg aa(newArg());
    QueryAction a(aa);
    BOOST_CHECK(a());
    
}


BOOST_AUTO_TEST_SUITE_END()
