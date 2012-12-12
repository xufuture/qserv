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
// QueryPlugin houses the implementation of the factory lookup code
// for QueryPlugins. 
#include "lsst/qserv/master/QueryPlugin.h"
#include "lsst/qserv/master/PluginNotFoundError.h"

#include <map>

namespace qMaster=lsst::qserv::master;
using lsst::qserv::master::QueryPlugin;

namespace { // File-scope helpers
typedef std::map<std::string, QueryPlugin::FactoryPtr> FactoryMap;

// Static local member
static FactoryMap factoryMap; 
}

QueryPlugin::Ptr
QueryPlugin::newInstance(std::string const& name) {
    FactoryMap::iterator e = factoryMap.find(name);
    if(e == factoryMap.end()) { // No plugin.
        throw PluginNotFoundError(name);
    } else {
        return e->second->newInstance();
    }
    
}

void
QueryPlugin::registerClass(QueryPlugin::FactoryPtr f) {
    if(!f.get()) return;    
    factoryMap[f->getName()] = f;
}
