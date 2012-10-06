/* 
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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

// SelectStmt is the query info structure. It contains information
// about the top-level query characteristics. It shouldn't contain
// information about run-time query execution.  It might contain
// enough information to generate queries for execution.


#if 0
// Standard
#include <functional>
#include <cstdio>
#include <strings.h>

#include <boost/bind.hpp>
#endif //comment out


// Standard
#include <map>
//#include <antlr/AST.hpp>

// Boost
//#include <boost/make_shared.hpp>

#include <boost/algorithm/string/predicate.hpp> // string iequal


// Local (placed in src/)
#include "SqlSQL2Parser.hpp" 
#include "SqlSQL2TokenTypes.hpp"

#include "lsst/qserv/master/parseTreeUtil.h"
#include "lsst/qserv/master/ColumnRefH.h"
#include "lsst/qserv/master/ColumnAliasMap.h"
#include "lsst/qserv/master/SelectList.h"

// myself
#include "lsst/qserv/master/SelectStmt.h"

// namespace modifiers
namespace qMaster = lsst::qserv::master;


////////////////////////////////////////////////////////////////////////
// Experimental
////////////////////////////////////////////////////////////////////////

// forward

////////////////////////////////////////////////////////////////////////
// class SelectStmt
////////////////////////////////////////////////////////////////////////

// Hook into parser to get populated.
qMaster::SelectStmt::SelectStmt() {
}

void qMaster::SelectStmt::diagnose() {
    //_selectList->getColumnRefList()->printRefs();
    _selectList->dbgPrint();
    _generate();
    
}

////////////////////////////////////////////////////////////////////////
// class SelectStmt (private)
////////////////////////////////////////////////////////////////////////
namespace {
template <typename OS, typename T>
inline OS& print(OS& os, char const label[], boost::shared_ptr<T> t) {
    if(t.get()) { 
        os << label << ": " << *t << std::endl;
    }
    return os; 
}
}
void qMaster::SelectStmt::_generate() {
    //_selectList->getColumnRefList()->printRefs();
    using std::cout;
    using std::endl;
    print(std::cout, "from", _fromList);
    print(std::cout, "select", _selectList);
    print(std::cout, "where", _whereClause);
    print(std::cout, "orderby", _orderBy);
    print(std::cout, "groupby", _groupBy);
    print(std::cout, "having", _having);
}
