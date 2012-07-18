// -*- LSST-C++ -*-
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
// SelectStmt contains extracted information about a particular parsed
// SQL select statement. It is not responsible for performing
// verification, validation, or other processing that requires
// persistent or run-time state.
#ifndef LSST_QSERV_MASTER_SELECTSTMT_H
#define LSST_QSERV_MASTER_SELECTSTMT_H

// Standard
#include <list>

// Forward
class SqlSQL2Parser;

namespace lsst {
namespace qserv {
namespace master {
// Forward
class SelectList;
class FromList;
class WhereClause;

/// class SelectStmt - a container for SQL SELECT statement info.
class SelectStmt  {
public:
    typedef boost::shared_ptr<SelectStmt> Ptr;
    typedef boost::shared_ptr<SelectStmt const> Cptr;
    typedef std::list<std::string> StringList; // placeholder
    
    SelectStmt();

    void addHooks(SqlSQL2Parser& p);

    void addSelectStar();

    void diagnose(); // for debugging

// private: // public for now.
    class Mgr;
    boost::shared_ptr<Mgr> _mgr;
    boost::shared_ptr<FromList> _fromList; // Data sources
    boost::shared_ptr<SelectList> _selectList; // Desired columns
    boost::shared_ptr<WhereClause> _whereClause; // Filtering conditions (WHERE)
    StringList OutputMods; // Output modifiers (order, grouping,
                           // sort, limit
};

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_SELECTSTMT_H
