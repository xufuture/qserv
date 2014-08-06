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

/// \file
/// \brief A class for resolving column references to table references

#ifndef LSST_QSERV_QANA_COLUMNVERTEXMAP_H
#define LSST_QSERV_QANA_COLUMNVERTEXMAP_H

// System headers
#include <string>
#include <vector>

// Third-party headers
#include <boost/shared_ptr.hpp>

// Local headers
#include "query/ColumnRef.h"


namespace lsst {
namespace qserv {
namespace qana {

typedef boost::shared_ptr<query::ColumnRef const> ColumnRefConstPtr;
class Vertex;

/// A `ColumnVertexMap` is a mapping from column references to the relation
/// graph vertices for the table references that column values can originate
/// from. Most of the time values come from a single table, but there are two
/// exceptions. Firstly, a column reference can be ambiguous, in which case its
/// presence in the query text must be treated as an error. Secondly, natural
/// join columns map to two or more table references. This is because the
/// result of `A NATURAL JOIN B` contains a single column `c` for every common
/// column `c` of `A` and `B`. Its value is `COALESCE(A.c, B.c)`, defined as
/// `CASE WHEN A.c IS NOT NULL THEN A.c ELSE B.c END`.
class ColumnVertexMap {
public:
    struct Entry {
        ColumnRefConstPtr cr;
        std::vector<Vertex*> vertices; // unowned

        Entry() {}
        Entry(ColumnRefConstPtr const& c, Vertex* t) :
            cr(c), vertices(1, t) {}
        void swap(Entry& e) {
            cr.swap(e.cr);
            vertices.swap(e.vertices);
        }
    };

    ColumnVertexMap() {}

    /// The constructor stores column references from a single table
    /// reference into a new `ColumnVertexMap`.
    explicit ColumnVertexMap(Vertex& v);

    void swap(ColumnVertexMap& m) {
        _entries.swap(m._entries);
    }

    /// `find` returns the vertices for table references corresponding to the
    /// given column reference. An empty vector is returned for unrecognized
    /// columns. If `c` is ambiguous, an exception is thrown.
    std::vector<Vertex*> const& find(query::ColumnRef const& c) const;

    /// `splice` transfers the entries of `m` to this map, emptying `m`.
    /// If `m` contains a column reference `c` that is already in this map,
    /// then `c` is marked ambiguous unless `c` is an unqualified reference,
    /// in which case behavior depends on the `natural` flag argument:
    ///
    /// - If `natural` is false, `c` is marked as ambiguous.
    /// - If `natural` is true, table references for `c` from both maps are
    ///   concatenated unless `c` is already ambiguous in either map, in which
    ///   case an exception is thrown.
    void splice(ColumnVertexMap& m, bool natural);

    /// `computeCommonColumns` returns all unqualified column names that are
    /// common to this map and `m`. If any such column is ambiguous in either
    /// map, an exception is thrown.
    std::vector<std::string> const computeCommonCols(
        ColumnVertexMap const& m) const;

private:
    // Not implemented
    ColumnVertexMap(ColumnVertexMap const&);
    ColumnVertexMap& operator=(ColumnVertexMap const&);

    std::vector<Entry> _entries; // sorted
};

}}} // namespace lsst::qserv::qana

#endif // LSST_QSERV_QANA_COLUMNVERTEXMAP_H
