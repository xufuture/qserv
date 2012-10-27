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
// PlanWriter writes SelectPlans from SelectStmts. It does this using
// a sequence of rewrite rules applied on the query and a parameter
// set derived with help from physical table information.
#include "lsst/qserv/master/PlanWriter.h"

#include <iostream>
#include "lsst/qserv/master/QueryTemplate.h"
#include "lsst/qserv/master/SelectStmt.h"


namespace qMaster=lsst::qserv::master;
using lsst::qserv::master::ChunkSpecList;
using lsst::qserv::master::PlanWriter;
using lsst::qserv::master::QueryTemplate;
using lsst::qserv::master::SelectPlan;
using lsst::qserv::master::SelectStmt;

namespace { // File-scope helpers
class MyMapping {
};
class Rule { // Operate on a SelectStmt in-place.
public:
    virtual ~Rule() {}
    virtual void operator()(SelectStmt& s) {}
};
class AlterUdf : public Rule {
public :
    virtual void operator()(SelectStmt& s) {
        // For restrictors in where clause
        // replace with value and funcexprs.        
    }
};
class MapPlan : public Rule {
public:
    virtual void operator()(SelectStmt& s) {
        // If aggregation, patch select list
        // and write new merge instructions
        // Otherwise, write simple merge instructions
        _mergeQt = s.getTemplate();
    }
    QueryTemplate const& getTemplate() const { return _mergeQt; };
private:
    QueryTemplate _mergeQt;
};
}

boost::shared_ptr<SelectPlan> 
PlanWriter::write(SelectStmt const& ss, ChunkSpecList const& specs) {
    boost::shared_ptr<SelectStmt> mapS = ss.copySyntax();
    QueryTemplate orig = ss.getTemplate();
    std::cout << "ORIGINAL: " << orig.dbgStr() << std::endl;
    MapPlan mp;
    mp(*mapS);
    std::cout << "MAPPED: " << mp.getTemplate().dbgStr() << std::endl;
    return boost::shared_ptr<SelectPlan>();
}
