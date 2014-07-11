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
#include "sql/statement.h"

// System headers
#include <sstream>

// Qserv headers
#include "sql/Schema.h"

namespace lsst {
namespace qserv {
namespace sql {

std::string formCreateTable(std::string const& table, sql::Schema const& s) {
    std::ostringstream os;
    os << "CREATE TABLE " << table << " (";
    ColumnsIter b, i, e;
    for(i=b=s.columns.begin(), e=s.columns.end(); i != e; ++i) {
        if(i != b) {
            os << ",\n";
        }
        os << *i;
    }
    os << ")";
    return os.str();
}

std::string formLoadInfile(std::string const& table, 
                           std::string const& virtFile) {
    std::ostringstream os;
    os << "LOAD DATA LOCAL INFILE '" << virtFile << "' INTO TABLE " 
       << table;
    return os.str();
}

}}} // namespace lsst::qserv::sql

