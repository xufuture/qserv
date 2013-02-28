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
// X is a ...

#ifndef LSST_QSERV_WORKER_MYSQLEXPORTMGR_H
#define LSST_QSERV_WORKER_MYSQLEXPORTMGR_H
#include <deque>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <boost/shared_ptr.hpp>

namespace lsst { namespace qserv { namespace worker {

class Logger; // Forward

class MySqlExportMgr {
public:
    typedef std::set<std::string> StringSet;
    typedef std::deque<std::string> StringDeque;
    typedef std::map<int,StringSet> ChunkMap; // convert to unordered_map
    typedef std::map<std::string, ChunkMap> ExistMap;

    MySqlExportMgr(std::string const& name, Logger& log) 
        : _name(name), _log(log) { 
        _init();
    }

    static inline std::string makeKey(std::string const& db, int chunk) {
        std::stringstream ss;
        ss << db << chunk << "**key";
        return std::string(ss.str());
    }
    static inline bool checkExist(StringSet const& s, 
                                  std::string const& db, int chunk) {
        std::string key = makeKey(db, chunk);
        return (s.end() != s.find(key));
    }

    void fillDbChunks(StringSet& s);
private:
    void _init();
    ExistMap _existMap;
    std::string _name;
    Logger& _log;
    
};
}}} // namespace lsst::qserv::worker


#endif // LSST_QSERV_WORKER_MYSQLEXPORTMGR_H
