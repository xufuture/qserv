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
/// \brief Table metadata class implementations.

#include "TableInfo.h"

// Third-party headers
#include <boost/make_shared.hpp>

// Local headers
#include "query/ColumnRef.h"

namespace lsst {
namespace qserv {
namespace qana {

namespace {

/// `appendColumnRefs` appends all possible references to the given
/// column to `columnRefs`. At most 3 references are appended.
void appendColumnRefs(std::string const& column,
                      std::string const& database,
                      std::string const& table,
                      std::string const& tableAlias,
                      std::vector<ColumnRefConstPtr>& refs)
{
    if (column.empty()) {
        return;
    }
    std::string const _;
    refs.push_back(boost::make_shared<query::ColumnRef>(_, _, column));
    if (!tableAlias.empty()) {
        // If a table alias has been introduced, then it is an error to
        // refer to a column using table.column or db.table.column
        refs.push_back(
            boost::make_shared<query::ColumnRef>(_, tableAlias, column));
    } else if (!table.empty()) {
        refs.push_back(
            boost::make_shared<query::ColumnRef>(_, table, column));
        if (!database.empty()) {
            refs.push_back(
                boost::make_shared<query::ColumnRef>(database, table, column));
        }
    }
}

} // unnamed namespace


std::string const TableInfo::CHUNK_TAG("%CC%");
std::string const TableInfo::SUBCHUNK_TAG("%SS%");

std::vector<ColumnRefConstPtr> const DirTableInfo::makeColumnRefs(
    std::string const& tableAlias) const
{
    std::vector<ColumnRefConstPtr> refs;
    refs.reserve(9);
    appendColumnRefs(pk, database, table, tableAlias, refs);
    appendColumnRefs(lon, database, table, tableAlias, refs);
    appendColumnRefs(lat, database, table, tableAlias, refs);
    return refs;
}

std::vector<ColumnRefConstPtr> const ChildTableInfo::makeColumnRefs(
    std::string const& tableAlias) const
{
    std::vector<ColumnRefConstPtr> refs;
    refs.reserve(3);
    appendColumnRefs(fk, database, table, tableAlias, refs);
    return refs;
}

std::vector<ColumnRefConstPtr> const MatchTableInfo::makeColumnRefs(
    std::string const& tableAlias) const
{
    std::vector<ColumnRefConstPtr> refs;
    refs.reserve(6);
    appendColumnRefs(fk.first, database, table, tableAlias, refs);
    appendColumnRefs(fk.second, database, table, tableAlias, refs);
    return refs;
}

}}} // namespace lsst::qserv::qana
