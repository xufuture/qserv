// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
  *
  * @brief unit tests for ccontrol module
  *
  */


// System headers
#include <iostream>
#include <map>

// Third-party headers
#include "boost/make_shared.hpp"
#include "boost/function.hpp"

// Qserv headers
#include "ccontrol/UserQuery.h"
#include "ccontrol/UserQueryFactory.h"
#include "css/Facade.h"
#include "css/KvInterfaceImplMem.h"
#include "proto/WorkerResponse.h"
#include "proto/worker.pb.h"
#include "qdisp/Executive.h"
#include "qdisp/MessageStore.h"
#include "qproc/IndexMap.h"
#include "qproc/QuerySession.h"
#include "qproc/SecondaryIndex.h"
#include "qproc/TaskMsgFactory2.h"
#include "rproc/InfileMerger.h"
#include "util/hippomocks.h"
#include "util/qmock.h"

// Boost unit test header
#define BOOST_TEST_MODULE ccontrol
#include "boost/test/included/unit_test.hpp"

namespace lsst {
namespace qserv {

namespace qdisp {

#define MOCK_METHODS_EXECUTIVE(MOCK_METHOD)                                    \
    MOCK_METHOD(Executive, add, 2, void(int refNum, Executive::Spec const& s)) \
    MOCK_METHOD(Executive, join, 0, bool())                                    \
    MOCK_METHOD(Executive, squash, 0, void())                                  \
    MOCK_METHOD(Executive, getNumInflight, 0, int())                           \
    MOCK_METHOD(Executive, getProgressDesc, 0, std::string(), const)           \

MOCK_DEFINE_MOCK(Executive, MOCK_METHODS_EXECUTIVE)
MOCK_DEFINE_STUBS(Executive, MOCK_METHODS_EXECUTIVE)

Executive::Executive(
    boost::shared_ptr<lsst::qserv::qdisp::Executive::Config> config,
    boost::shared_ptr<lsst::qserv::qdisp::MessageStore>)
:
    _config(*config),
    _empty(true)
{
}

#define MOCK_METHODS_MESSAGESTORE(MOCK_METHOD)                                                            \
    MOCK_METHOD(MessageStore, addMessage, 3, void(int chunkId, int code, std::string const& description)) \
    MOCK_METHOD(MessageStore, getMessage, 1, const QueryMessage(int idx))                                 \
    MOCK_METHOD(MessageStore, messageCount, 0, const int())                                               \

MOCK_DEFINE_MOCK(MessageStore, MOCK_METHODS_MESSAGESTORE)
MOCK_DEFINE_STUBS(MessageStore, MOCK_METHODS_MESSAGESTORE)

} // namespace qdisp

namespace qproc {

#define MOCK_METHODS_INDEXMAP(MOCK_METHOD)                                                            \
    MOCK_METHOD(IndexMap, getAll, 0, ChunkSpecVector())                                               \
    MOCK_METHOD(IndexMap, getIntersect, 1, ChunkSpecVector(query::ConstraintVector const& cv))        \

MOCK_DEFINE_MOCK(IndexMap, MOCK_METHODS_INDEXMAP)
MOCK_DEFINE_STUBS(IndexMap, MOCK_METHODS_INDEXMAP)

IndexMap::IndexMap(
    lsst::qserv::css::StripingParams const&,
    boost::shared_ptr<lsst::qserv::qproc::SecondaryIndex>)
{
}

#define MOCK_METHODS_QUERYSESSION(MOCK_METHOD)                                                        \
    MOCK_METHOD(QuerySession, setDefaultDb, 1, void(std::string const& db))                           \
    MOCK_METHOD(QuerySession, setQuery, 1, void(std::string const& q))                                \
    MOCK_METHOD(QuerySession, hasChunks, 0, bool(), const)                                            \
    MOCK_METHOD(QuerySession, getConstraints, 0, boost::shared_ptr<query::ConstraintVector>(), const) \
    MOCK_METHOD(QuerySession, addChunk, 1, void(ChunkSpec const& cs))                                 \
    MOCK_METHOD(QuerySession, setResultTable, 1, void(std::string const& resultTable))                \
    MOCK_METHOD(QuerySession, getDominantDb, 0, std::string(), const)                                 \
    MOCK_METHOD(QuerySession, validateDominantDb, 0, bool(), const)                                   \
    MOCK_METHOD(QuerySession, getDbStriping, 0, css::StripingParams())                                \
    MOCK_METHOD(QuerySession, getEmptyChunks, 0, boost::shared_ptr<IntSet const>())                   \
    MOCK_METHOD(QuerySession, getMergeStmt, 0, boost::shared_ptr<query::SelectStmt>(), const)         \
    MOCK_METHOD(QuerySession, finalize, 0, void())                                                    \
    MOCK_METHOD(QuerySession, cQueryBegin, 0, QuerySession::Iter())                                   \
    MOCK_METHOD(QuerySession, cQueryEnd, 0, QuerySession::Iter())                                     \

MOCK_DEFINE_MOCK(QuerySession, MOCK_METHODS_QUERYSESSION)
MOCK_DEFINE_STUBS(QuerySession, MOCK_METHODS_QUERYSESSION)

QuerySession::QuerySession(boost::shared_ptr<lsst::qserv::css::Facade>)
{
}

ChunkQuerySpec& QuerySession::Iter::dereference() const
{
    static ChunkQuerySpec spec;
    return spec;
}

#define MOCK_METHODS_TASKMSGFACTORY2(MOCK_METHOD)                                                                                      \
    MOCK_METHOD(TaskMsgFactory2, serializeMsg, 3, void(ChunkQuerySpec const& s, std::string const& chunkResultName, std::ostream& os)) \

MOCK_DEFINE_MOCK(TaskMsgFactory2, MOCK_METHODS_TASKMSGFACTORY2)
MOCK_DEFINE_STUBS(TaskMsgFactory2, MOCK_METHODS_TASKMSGFACTORY2)

TaskMsgFactory2::TaskMsgFactory2(int)
{
}

SecondaryIndex::SecondaryIndex(lsst::qserv::mysql::MySqlConfig const&)
{
}

} // namespace qproc

namespace rproc {

#define MOCK_METHODS_INFILEMERGER(MOCK_METHOD)                                                   \
    MOCK_METHOD(InfileMerger, merge, 1, bool(boost::shared_ptr<proto::WorkerResponse> response)) \
    MOCK_METHOD(InfileMerger, finalize, 0, bool())                                               \
    MOCK_METHOD(InfileMerger, isFinished, 0, bool(), const)                                      \

MOCK_DEFINE_MOCK(InfileMerger, MOCK_METHODS_INFILEMERGER)
MOCK_DEFINE_STUBS(InfileMerger, MOCK_METHODS_INFILEMERGER)

InfileMerger::InfileMerger(lsst::qserv::rproc::InfileMergerConfig const&)
{
}

InfileMerger::~InfileMerger()
{
}

} // namespace rproc

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_AUTO_TEST_CASE(Trivial) {

    std::map<std::string, std::string> config = {
        {"/css_meta/version", "1"}
    };

    auto kvi = boost::make_shared<css::KvInterfaceImplMem>(std::move(config));
    auto uqf = new ccontrol::UserQueryFactory(config, kvi);
    auto uqId = uqf->newUserQuery("query", "db", "results");

    std::cout << uqId << std::endl;

}

BOOST_AUTO_TEST_SUITE_END()

}} // namespace lsst::qserv
