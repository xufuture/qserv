// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
// A QuerySession contains information regarding a top-level query, including
// the text of the original query, a parsed query tree, and other user
// state/context. 

#ifndef LSST_QSERV_MASTER_QUERYSESSION_H
#define LSST_QSERV_MASTER_QUERYSESSION_H
#include <string>
#include "lsst/qserv/master/transaction.h"

namespace lsst { namespace qserv { namespace master {


class QuerySession {
public:
    friend class AsyncQueryManager; // factory for QuerySession.
    void setQuery(std::string const& q);
    ConstraintVector getConstraints() const;
    void addChunk(ChunkSpec const& cs);
private:
    QuerySession();
};

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_QUERYSESSION_H

