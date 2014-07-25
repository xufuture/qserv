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

namespace lsst {
namespace qserv {
namespace qana {

using std::string;
using std::vector;
using query::ColumnRef;


namespace {

    /// `addColumnRefs` appends all possible references to `column`
    /// from the given table to `columnRefs`.
    void addColumnRefs(string const& column,
                       string const& database,
                       string const& table,
                       string const& alias,
                       vector<ColumnRef>& columnRefs)
    {
        if (column.empty()) {
            return;
        }
        string const _;
        ColumnRef cr(_, _, column);
        columnRefs.push_back(cr);
        if (!alias.empty()) {
            // If an alias has been introduced, then it is an error to refer
            // to a column using table.column or db.table.column .
            cr.table = alias;
            columnRefs.push_back(cr);
        } else if (!table.empty()) {
            cr.table = table;
            columnRefs.push_back(cr);
            if (!database.empty()) {
                cr.db = database;
                columnRefs.push_back(cr);
            }
        }
    }

} // unnamed namespace


vector<ColumnRef> const DirTableInfo::getColumnRefs(
    string const& alias) const
{
    vector<ColumnRef> columnRefs;
    columnRefs.reserve(9);
    addColumnRefs(pk, database, table, alias, columnRefs);
    addColumnRefs(lon, database, table, alias, columnRefs);
    addColumnRefs(lat, database, table, alias, columnRefs);
    return columnRefs;
}

vector<ColumnRef> const ChildTableInfo::getColumnRefs(
    string const& alias) const
{
    vector<ColumnRef> columnRefs;
    columnRefs.reserve(3);
    addColumnRefs(fk, database, table, alias, columnRefs);
    return columnRefs;
}

vector<ColumnRef> const MatchTableInfo::getColumnRefs(
    string const& alias) const
{
    vector<ColumnRef> columnRefs;
    columnRefs.reserve(6);
    addColumnRefs(fk.first, database, table, alias, columnRefs);
    addColumnRefs(fk.second, database, table, alias, columnRefs);
    return columnRefs;
}

}}} // namespace lsst::qserv::qana
