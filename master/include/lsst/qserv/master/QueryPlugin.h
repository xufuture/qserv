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
// QueryPlugin is an interface for classes which implement rewrite/optimization
// rules for incoming SQL queries.  Plugins can act upon the intermediate
// representation or the concrete plan or both.

#ifndef LSST_QSERV_MASTER_QUERYPLUGIN_H
#define LSST_QSERV_MASTER_QUERYPLUGIN_H
#include <boost/shared_ptr.hpp>

namespace lsst { namespace qserv { namespace master {
class SelectStmt; // Forward

class QueryPlugin {
public:
    // Types
    class Factory;
    typedef boost::shared_ptr<QueryPlugin> Ptr;
    typedef boost::shared_ptr<Factory> FactoryPtr;
    
    virtual ~QueryPlugin() {}

    /// Prepare the plugin for a query
    virtual void prepare() {} 

    /// Apply the plugin's actions to the parsed, but not planned query
    virtual void applyLogical(SelectStmt& stmt) {}

    /// Apply the plugins's actions to the concrete query plan.
    virtual void applyPhysical(SelectStmt& stmt) {} // FIXME

    static Ptr newInstance(std::string const& name);
    static void registerClass(FactoryPtr f);
};

class QueryPlugin::Factory {
public:
    // Types
    typedef boost::shared_ptr<Factory> Ptr;
    
    virtual ~Factory() {}

    virtual std::string getName() const { return std::string(); }
    virtual QueryPlugin::Ptr newInstance();
};

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_QUERYPLUGIN_H

