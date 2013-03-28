// -*- LSST-C++ -*-
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
// QueryMapping is a value class that stores a mapping that can be
// consulted for a partitioning-strategy-agnostic query generation
// stage that substitutes real table names for placeholders, according
// to a query's specified partition coverage.

#ifndef LSST_QSERV_MASTER_QUERYMAPPING_H
#define LSST_QSERV_MASTER_QUERYMAPPING_H
#include <boost/shared_ptr.hpp>

namespace lsst { namespace qserv { namespace master {

class QueryMapping {
public:
    typedef boost::shared_ptr<QueryMapping> Ptr;
    explicit QueryMapping(int a) {} // FIXME
    void update(QueryMapping const& qm) {} // FIXME
private:
};
}}} // namespace lsst::qserv::master

#endif // LSST_QSERV_MASTER_QUERYMAPPING_H

