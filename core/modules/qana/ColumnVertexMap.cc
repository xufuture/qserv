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

/// \file
/// \brief Implementation of column to table reference resolution

#include "ColumnVertexMap.h"

// System headers
#include <algorithm>
#include <utility>

// Local headers
#include "QueryNotEvaluableError.h"
#include "RelationGraph.h"

#include "query/QueryTemplate.h"


namespace lsst {
namespace qserv {
namespace qana {

ColumnVertexMap::ColumnVertexMap(Vertex& v) {
    std::vector<ColumnRefConstPtr> c = v.info->makeColumnRefs(v.tr.getAlias());
    _init(v, c.begin(), c.end());
}

std::vector<Vertex*> const& ColumnVertexMap::find(
    query::ColumnRef const& c) const
{
    typedef std::vector<Entry>::const_iterator Iter;
    std::vector<Vertex*> static const NONE;

    std::pair<Iter,Iter> p = std::equal_range(
        _entries.begin(), _entries.end(), c, ColumnRefLt());
    if (p.first == p.second) {
        return NONE;
    } else if (p.first->vertices.empty()) {
        query::QueryTemplate qt;
        c.renderTo(qt);
        throw QueryNotEvaluableError("Column reference " + qt.generate() +
                                     " is ambiguous");
    }
    return p.first->vertices;
}

void ColumnVertexMap::splice(ColumnVertexMap& m,
                             bool natural,
                             std::vector<std::string> const& cols)
{
    typedef std::vector<Entry>::iterator EntryIter;
    typedef std::vector<std::string>::const_iterator StringIter;

    std::vector<Entry>::size_type s = _entries.size();
    // Reserve required space up front, then swap default constructed
    // entries with entries from m.
    _entries.resize(s + m._entries.size());
    EntryIter middle = _entries.begin() + s;
    for (EntryIter i = m._entries.begin(), e = m._entries.end(), o = middle;
         i != e; ++i, ++o) {
        o->swap(*i);
    }
    m._entries.clear();
    // Merge-sort the result
    std::inplace_merge(_entries.begin(), middle, _entries.end(), ColumnRefLt());
    // Remove duplicate column references
    if (!_entries.empty()) {
        ColumnRefEq eq;
        EntryIter o = _entries.begin(), i = o, e = _entries.end();
        StringIter cb = cols.begin(), ce = cols.end();
        while (++i != e) {
            if (eq(*o, *i)) {
                if (!o->cr->table.empty() ||
                    (!natural && std::find(cb, ce, o->cr->column) == ce)) {
                    // duplicate is a qualified column reference
                    // or is not a natural join or using column
                    o->vertices.clear();
                } else if (o->vertices.empty() || i->vertices.empty()) {
                    throw QueryNotEvaluableError(
                        "Join column " + o->cr->column + " is ambiguous");
                } else {
                    // concatenate table references for natural join column
                    o->vertices.insert(o->vertices.end(),
                                       i->vertices.begin(),
                                       i->vertices.end());
                }
            } else {
                *(++o) = *i;
            }
        }
        _entries.erase(++o, _entries.end());
    }
}

std::vector<std::string> const ColumnVertexMap::computeCommonColumns(
    ColumnVertexMap const& m) const
{
    typedef std::vector<Entry>::const_iterator EntryIter;
    std::vector<std::string> cols;
    ColumnRefLt lt;
    EntryIter i = _entries.begin(), iend = _entries.end();
    EntryIter j = m._entries.begin(), jend = m._entries.end();
    while (i != iend && j != jend) {
        if (lt(*i, *j)) {
            ++i;
        } else if (lt(*j, *i)) {
            ++j;
        } else {
            if (i->cr->table.empty()) {
                // unqualified column reference
                if (i->vertices.empty() || j->vertices.empty()) {
                    throw QueryNotEvaluableError(
                        "Join column " + i->cr->column + " is ambiguous");
                }
                cols.push_back(i->cr->column);
            }
            ++i;
            ++j;
        }
    }
    return cols;
}

}}} // namespace lsst::qserv::qana
