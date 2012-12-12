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
// X houses the implementation of 
#include "lsst/qserv/master/AggOp.h"

namespace qMaster=lsst::qserv::master;
using lsst::qserv::master::AggOp;
using lsst::qserv::master::AggRecord;
using lsst::qserv::master::ValueExpr;

namespace { // File-scope helpers
}
////////////////////////////////////////////////////////////////////////
// AggOp specializations
////////////////////////////////////////////////////////////////////////
class PassAggOp : public AggOp {
public:
    virtual AggRecord::Ptr operator()(ValueExpr& orig) {} // FIXME!
};
class CountAggOp : public AggOp {
public:
    virtual AggRecord::Ptr operator()(ValueExpr& orig) {}
};
class AvgAggOp : public AggOp {
public:
    virtual AggRecord::Ptr operator()(ValueExpr& orig) {}
};

////////////////////////////////////////////////////////////////////////
// class AggOp::Mgr
////////////////////////////////////////////////////////////////////////
AggOp::Mgr::Mgr() {
    // Load the map
    _map["count"].reset(new CountAggOp());
    _map["avg"].reset(new AvgAggOp());
    _map["max"].reset(new PassAggOp());
    _map["min"].reset(new PassAggOp());
    _map["sum"].reset(new PassAggOp());
}

AggOp::Ptr 
AggOp::Mgr::getOp(std::string const& name) {
    OpMap::const_iterator i = _map.find(name);
    if(i != _map.end()) return i->second;
    else return AggOp::Ptr();
}

AggRecord::Ptr 
AggOp::Mgr::applyOp(std::string const& name, ValueExpr const& orig) {
    AggOp::Ptr p = getOp(name);
    assert(p.get());
    return (*p)(orig);
}

