/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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

#include <stdexcept>

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE HtmIndex
#include "boost/test/unit_test.hpp"

#include "Constants.h"
#include "FileUtils.h"
#include "HtmIndex.h"
#include "TempFile.h"

namespace fs = boost::filesystem;
namespace dupr = lsst::qserv::admin::dupr;

using std::exception;
using std::vector;
using dupr::HtmIndex;
using dupr::HTM_MAX_LEVEL;

namespace {
    bool operator==(HtmIndex::Triangle const & t1,
                    HtmIndex::Triangle const & t2) {
        return t1.id == t2.id &&
               t1.numRecords == t2.numRecords &&
               t1.recordSize == t2.recordSize;
    }
}

BOOST_AUTO_TEST_CASE(HtmIndexTest) {
    BOOST_CHECK_THROW(HtmIndex(-1), exception);
    BOOST_CHECK_THROW(HtmIndex(HTM_MAX_LEVEL + 1), exception);
    HtmIndex idx(HTM_MAX_LEVEL);
    HtmIndex::Triangle t;
    t.id = static_cast<uint32_t>(0x8) << (2*HTM_MAX_LEVEL);
    t.numRecords = 1;
    t.recordSize = 10;
    BOOST_CHECK_EQUAL(idx.size(), 0u);
    BOOST_CHECK(idx.empty());
    BOOST_CHECK_EQUAL(idx.getLevel(), HTM_MAX_LEVEL);
    BOOST_CHECK_EQUAL(idx.getNumRecords(), 0u);
    BOOST_CHECK_EQUAL(idx.getRecordSize(), 0u);
    BOOST_CHECK_THROW(idx.mapToNonEmpty(t.id), exception);
    BOOST_CHECK_EQUAL(idx(t.id).id, 0u);
    BOOST_CHECK_EQUAL(idx(t.id).numRecords, 0u);
    BOOST_CHECK_EQUAL(idx(t.id).recordSize, 0u);
    idx.merge(t);
    BOOST_CHECK_EQUAL(idx.size(), 1u);
    BOOST_CHECK(!idx.empty());
    BOOST_CHECK_EQUAL(idx.getNumRecords(), t.numRecords);
    BOOST_CHECK_EQUAL(idx.getRecordSize(), t.recordSize);
    BOOST_CHECK(idx.mapToNonEmpty(t.id) == t);
    BOOST_CHECK(idx.mapToNonEmpty(1234) == t);
    BOOST_CHECK(idx(t.id) == t);
    idx.merge(t);
    t.id += 1;
    idx.merge(t);
    BOOST_CHECK_EQUAL(idx.size(), 2u);
    BOOST_CHECK_EQUAL(idx.getNumRecords(), 3u);
    BOOST_CHECK_EQUAL(idx.getRecordSize(), 30u);
    BOOST_CHECK_EQUAL(idx(t.id - 1).numRecords, 2u);
    BOOST_CHECK_EQUAL(idx(t.id - 1).recordSize, 20u);
    BOOST_CHECK(idx(t.id) == t);
    idx.clear();
    BOOST_CHECK_EQUAL(idx.size(), 0u);
    BOOST_CHECK(idx.empty());
    BOOST_CHECK_THROW(idx.mapToNonEmpty(t.id), exception);
    BOOST_CHECK_EQUAL(idx(t.id).numRecords, 0u);
}

BOOST_AUTO_TEST_CASE(HtmIndexMergeTest) {
    HtmIndex i1(2);
    HtmIndex i2(2);
    HtmIndex i3(HTM_MAX_LEVEL);
    HtmIndex::Triangle t;
    BOOST_CHECK_THROW(i1.merge(i3), exception);
    t.id = 0x80;
    t.numRecords = 3;
    t.recordSize = 456;
    i1.merge(t);
    t.id = 0xf2;
    i1.merge(t);
    i2.merge(t);
    t.id = 0x93;
    i2.merge(t);
    i1.merge(i2);
    BOOST_CHECK_EQUAL(i1.size(), 3u);
    BOOST_CHECK_EQUAL(i1.getNumRecords(), 12u);
    BOOST_CHECK_EQUAL(i1.getRecordSize(), 456u*4u);
    BOOST_CHECK(i1(t.id) == t);
    t.id = 0x80;
    BOOST_CHECK(i1(t.id) == t);
    t.id = 0xf2;
    t.numRecords *= 2;
    t.recordSize *= 2;
    BOOST_CHECK(i1(t.id) == t);
}

BOOST_AUTO_TEST_CASE(HtmIndexIoTest) {
    HtmIndex i1(2), i2(2), i3(2), i4(4);
    TempFile t1, t2, t3;
    HtmIndex::Triangle t;
    t.id = 0x80;
    t.numRecords = 1;
    t.recordSize = 10;
    i1.merge(t);
    t.id = 0x8f;
    i1.merge(t);
    i2.merge(t);
    t.id = 0xc3;
    i2.merge(t);
    t.id = 0x800;
    i4.merge(t);
    i1.write(t1.path(), false);
    i2.write(t2.path(), false);
    i4.write(t3.path(), false);
    i3 = HtmIndex(t1.path());
    BOOST_CHECK_EQUAL(i1.size(), i3.size());
    BOOST_CHECK_EQUAL(i1.getNumRecords(), i3.getNumRecords());
    BOOST_CHECK_EQUAL(i1.getRecordSize(), i3.getRecordSize());
    BOOST_CHECK(i1(0x80) == i3(0x80));
    BOOST_CHECK(i1(0x8f) == i3(0x8f));
    i3 = HtmIndex(2);
    i3.merge(i1);
    i3.merge(i2);
    vector<fs::path> v;
    v.push_back(t1.path());
    v.push_back(t2.path());
    i4 = HtmIndex(v);
    BOOST_CHECK_EQUAL(i3.size(), i4.size());
    BOOST_CHECK_EQUAL(i3.getNumRecords(), i4.getNumRecords());
    BOOST_CHECK_EQUAL(i3.getRecordSize(), i4.getRecordSize());
    BOOST_CHECK(i3(0x80) == i4(0x80));
    BOOST_CHECK(i3(0x8f) == i4(0x8f));
    BOOST_CHECK(i3(0xc3) == i4(0xc3));
    // t3 contains level 4 indexes, while t1 and t2 contain level 2 indexes.
    v.push_back(t3.path());
    BOOST_CHECK_THROW((HtmIndex(v)), exception);
    // Check that the concatenation of temporary files 1 and 2 is equivalent
    // to the merge of both indexes.
    t3.concatenate(t1, t2);
    i4 = HtmIndex(t3.path());
    BOOST_CHECK_EQUAL(i3.size(), i4.size());
    BOOST_CHECK_EQUAL(i3.getNumRecords(), i4.getNumRecords());
    BOOST_CHECK_EQUAL(i3.getRecordSize(), i4.getRecordSize());
    BOOST_CHECK(i3(0x80) == i4(0x80));
    BOOST_CHECK(i3(0x8f) == i4(0x8f));
    BOOST_CHECK(i3(0xc3) == i4(0xc3));
}
