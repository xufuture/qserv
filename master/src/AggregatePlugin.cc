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
// AggregatePlugin houses the implementation of an implementation of a
// QueryPlugin that primarily operates in the second phase of query
// manipulation. It rewrites the select-list of a query in their
// parallel and merging instances so that a SUM() becomes a SUM()
// followed by another SUM(), AVG() becomes SUM() and COUNT() followed
// by SUM()/SUM(), etc. 

#include "lsst/qserv/master/AggregatePlugin.h"
#include <string>
#include "lsst/qserv/master/QueryContext.h"
#include "lsst/qserv/master/QueryPlugin.h"
#include "lsst/qserv/master/QueryTemplate.h"
#include "lsst/qserv/master/ValueExpr.h"
#include "lsst/qserv/master/FuncExpr.h"

#include "lsst/qserv/master/SelectList.h"
#include "lsst/qserv/master/SelectStmt.h"
#include "lsst/qserv/master/AggOp.h"

namespace qMaster=lsst::qserv::master;
using lsst::qserv::master::QueryContext;
using lsst::qserv::master::QueryPlugin;
using lsst::qserv::master::QueryTemplate;
using lsst::qserv::master::ValueExpr;
using lsst::qserv::master::FuncExpr;
using lsst::qserv::master::AggOp;
using lsst::qserv::master::AggRecord;

namespace { // File-scope helpers
template <class C>
void printList(char const* label, C const& c) {
    typename C::const_iterator i; 
    std::cout << label << ": ";
    for(i = c.begin(); i != c.end(); ++i) {
        std::cout << **i << ", ";
    }
    std::cout << std::endl;
}

template <class C>
class convertAgg {
public:
    typedef typename C::value_type T;
    convertAgg(C& pList_, C& mList_, AggOp::Mgr& aMgr_) 
        : pList(pList_), mList(mList_), aMgr(aMgr_) {}
    void operator()(T const& e) {
        _makeRecord(*e);
    }

private:
    void _makeRecord(lsst::qserv::master::ValueExpr const& e) {
        if(e.getType() != ValueExpr::AGGFUNC) { 
            pList.push_back(e.clone()); // Is this sufficient?
            return; 
        }
        AggRecord r;
        r.orig = e.clone();
        assert(e.getFuncExpr().get());        
        AggRecord::Ptr p = aMgr.applyOp(e.getFuncExpr()->name, e);
        assert(p.get());
        pList.insert(pList.end(), p->pass.begin(), p->pass.end());
        mList.insert(mList.end(), p->fixup.begin(), p->fixup.end());
    }
    C& pList;
    C& mList;
    AggOp::Mgr& aMgr;
};

} // anonymous

////////////////////////////////////////////////////////////////////////
// AggregatePlugin declaration
////////////////////////////////////////////////////////////////////////
class AggregatePlugin : public lsst::qserv::master::QueryPlugin {
public:
    // Types
    typedef boost::shared_ptr<AggregatePlugin> Ptr;
    
    virtual ~AggregatePlugin() {}

    /// Prepare the plugin for a query
    virtual void prepare() {}

    /// Apply the plugin's actions to the parsed, but not planned query
    virtual void applyLogical(lsst::qserv::master::SelectStmt& stmt,
                              QueryContext&) {}

    /// Apply the plugins's actions to the concrete query plan.
    virtual void applyPhysical(lsst::qserv::master::QueryPlugin::Plan& p,
                               QueryContext&);
private:
    AggOp::Mgr _aMgr;
};

////////////////////////////////////////////////////////////////////////
// AggregatePluginFactory declaration+implementation
////////////////////////////////////////////////////////////////////////
class AggregatePluginFactory : public lsst::qserv::master::QueryPlugin::Factory {
public:
    // Types
    typedef boost::shared_ptr<AggregatePluginFactory> Ptr;
    AggregatePluginFactory() {}
    virtual ~AggregatePluginFactory() {}

    virtual std::string getName() const { return "Aggregate"; }
    virtual lsst::qserv::master::QueryPlugin::Ptr newInstance() {
        return lsst::qserv::master::QueryPlugin::Ptr(new AggregatePlugin());
    }
};

////////////////////////////////////////////////////////////////////////
// registarAggregatePlugin implementation
////////////////////////////////////////////////////////////////////////
// factory registration
void 
lsst::qserv::master::registerAggregatePlugin() {
    AggregatePluginFactory::Ptr f(new AggregatePluginFactory());
    lsst::qserv::master::QueryPlugin::registerClass(f);
}

////////////////////////////////////////////////////////////////////////
// AggregatePlugin implementation
////////////////////////////////////////////////////////////////////////
void
AggregatePlugin::applyPhysical(QueryPlugin::Plan& p, 
                               QueryContext&  context) {
    using lsst::qserv::master::SelectList;
    // For each entry in original's SelectList, modify the SelectList
    // for the parallel and merge versions.
    // Set hasMerge to true if aggregation is detected.
    SelectList& oList = p.stmtOriginal.getSelectList();
    SelectList& pList = p.stmtParallel.getSelectList();
    SelectList& mList = p.stmtMerge.getSelectList();
    boost::shared_ptr<qMaster::ValueExprList> vlist;
    vlist = oList.getValueExprList();
    assert(vlist.get());
    
    printList("aggr origlist", *vlist);
    // Clear out select lists, since we are rewriting them.
    pList.getValueExprList()->clear();
    mList.getValueExprList()->clear();
    AggOp::Mgr m; // Eventually, this can be shared?
    convertAgg<qMaster::ValueExprList> ca(*pList.getValueExprList(), 
                                          *mList.getValueExprList(),
                                          m);
    
    std::for_each(vlist->begin(), vlist->end(), ca);

    QueryTemplate qt;
    pList.renderTo(qt);
    std::cout << "pass: " << qt.dbgStr() << std::endl;
    qt.clear();
    mList.renderTo(qt);
    std::cout << "fixup: " << qt.dbgStr() << std::endl;

    // Also need to operate on GROUP BY.
    // update context.
    if(m.hasAggregate()) { context.needsMerge = true; }
}
