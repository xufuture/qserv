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
#include "lsst/qserv/master/AggOp.h"

#include <sstream>
#include "lsst/qserv/master/FuncExpr.h"
#include "lsst/qserv/master/ValueExpr.h"
#include "lsst/qserv/master/ValueTerm.h"

namespace qMaster=lsst::qserv::master;
using lsst::qserv::master::AggOp;
using lsst::qserv::master::AggRecord;
using lsst::qserv::master::ValueExpr;
using lsst::qserv::master::ValueTerm;
using lsst::qserv::master::FuncExpr;

namespace { // File-scope helpers

}
namespace lsst { namespace qserv {namespace master {
////////////////////////////////////////////////////////////////////////
// AggOp specializations
////////////////////////////////////////////////////////////////////////
class PassAggOp : public AggOp {
public:
    explicit PassAggOp(AggOp::Mgr& mgr) : AggOp(mgr) {}

    virtual AggRecord2::Ptr operator()(ValueTerm const& orig) {
        AggRecord2::Ptr arp(new AggRecord2());
        arp->orig = orig.clone();
        arp->pass.push_back(ValueExpr::newSimple(orig.clone()));
        arp->fixup = orig.clone();
        // Alias handling left to caller.
        return arp;
    } 
};
class CountAggOp : public AggOp {
public:
    explicit CountAggOp(AggOp::Mgr& mgr) : AggOp(mgr) {}

    virtual AggRecord2::Ptr operator()(ValueTerm const& orig) {
        AggRecord2::Ptr arp(new AggRecord2());
        std::string interName = _mgr.getAggName("COUNT");
        arp->orig = orig.clone();
        boost::shared_ptr<FuncExpr> fe;
        boost::shared_ptr<ValueTerm> vt;
        boost::shared_ptr<ValueExpr> passExpr;
        
        passExpr = ValueExpr::newSimple(orig.clone());
        passExpr->setAlias(interName);
        arp->pass.push_back(passExpr);

        fe = FuncExpr::newArg1("SUM", interName);
        vt = ValueTerm::newFuncTerm(fe);
        // orig alias handled by caller.
        arp->fixup = vt;
        return arp;
    }
};
class AvgAggOp : public AggOp {
public:
    explicit AvgAggOp(AggOp::Mgr& mgr) : AggOp(mgr) {}

    virtual AggRecord2::Ptr operator()(ValueTerm const& orig) {
        return AggRecord2::Ptr();
# if 0
        AggRecord::Ptr arp(new AggRecord());
        arp->orig = orig.clone();

        // FIXME: For each term in orig.
        boost::shared_ptr<FuncExpr> fe;
        boost::shared_ptr<ValueTerm const> origVt(orig);
        boost::shared_ptr<ValueExpr> ve;
        std::string cName = _mgr.getAggName("COUNT");
        fe = FuncExpr::newLike(*origVt->getFuncExpr(), "COUNT");
        ve = ValueExpr::newSimple(ValueTerm::newFuncTerm(fe));
        ve->setAlias(cName);
        arp->pass.push_back(ve);

        std::string sName = _mgr.getAggName("SUM");
        fe = FuncExpr::newLike(*origVt->getFuncExpr(), "SUM");
        ve = ValueExpr::newSimple(ValueTerm::newFuncTerm(fe));
        ve->setAlias(sName);
        arp->pass.push_back(ve);
        
        boost::shared_ptr<FuncExpr> fe1;
        boost::shared_ptr<FuncExpr> fe2;
        fe1 = FuncExpr::newArg1("SUM", sName);
        fe2 = FuncExpr::newArg1("SUM", cName);

#if 0        // FIXME!!
        // ArithExpr::newExpr(fe1,fe2);
#else // Broken placeholder
        ve = ValueExpr::newSimple(ValueTerm::newFuncTerm(fe1));
        ve->setAlias(orig.getAlias());
        arp->fixup.push_back(ve);
#endif

        arp->origAlias = orig.getAlias();
        return arp;
#endif
    }
};

////////////////////////////////////////////////////////////////////////
// class AggOp::Mgr
////////////////////////////////////////////////////////////////////////
AggOp::Mgr::Mgr() {
    // Load the map
    AggOp::Ptr pass(new PassAggOp(*this));
    _map["COUNT"].reset(new CountAggOp(*this));
    _map["AVG"].reset(new AvgAggOp(*this));
    _map["MAX"] = pass;
    _map["MIN"] = pass;
    _map["SUM"] = pass;
    _seq = 0; // Note: accessor return ++_seq
}

AggOp::Ptr 
AggOp::Mgr::getOp(std::string const& name) {
    OpMap::const_iterator i = _map.find(name);
    if(i != _map.end()) return i->second;
    else return AggOp::Ptr();
}

AggRecord2::Ptr 
AggOp::Mgr::applyOp(std::string const& name, ValueTerm const& orig) {
    std::string n(name);
    std::transform(name.begin(), name.end(), n.begin(), ::toupper);
    AggOp::Ptr p = getOp(n);
    assert(p.get());
    return (*p)(orig);
}

std::string AggOp::Mgr::getAggName(std::string const& name) {
    std::stringstream ss;
    int s = getNextSeq();
    ss << "QS" << s << "_" << name;
    return ss.str();
}
}}} // lsst::qserv::master
