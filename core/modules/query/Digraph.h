// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2015 AURA/LSST.
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

#ifndef LSST_QSERV_QUERY_DIGRAPH_H
#define LSST_QSERV_QUERY_DIGRAPH_H
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <iostream>

// Third-party headers
#include "boost/shared_ptr.hpp"

// Local headers
#include "global/stringTypes.h"
#include "query/QueryTemplate.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace parser {
    class SelectFactory;
}

namespace query {
class Digraph {
    Digraph() {}
    
    void addNode(uint64_t id, std::string const& descr);
    void addNode(void* ptr, std::string const& descr);

    void addEdge(uint64_t src, uint64_t dest);
    void addEdge(void* srcP, void* destP);
    
    template<class T>
    inline void addLinkedNode(void* srcP, T* destP) {
        if(srcP && destP) {
            dg.addEdge(srcP, destP);
            destP->writeDigraphNode(dg);
        }
    }
}

    void emitDot(std::ostream& os);
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_SELECTSTMT_H
