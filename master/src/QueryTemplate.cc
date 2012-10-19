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
// QueryTemplate houses the implementation of QueryTemplate, which is
// a object that can be used to generate concrete queries from a
// template, given certain parameters (e.g. chunk/subchunk).
#include "lsst/qserv/master/QueryTemplate.h"
#include <sstream>
#include "lsst/qserv/master/sqltoken.h" // sqlShouldSeparate

namespace qMaster=lsst::qserv::master;

namespace { // File-scope helpers
struct SpacedOutput {
    SpacedOutput(std::ostream& os_, std::string sep_=" ") 
        : os(os_), sep(sep_) {}
    void operator()(std::string const& s) {
        if(s.empty()) return;
        if(qMaster::sqlShouldSeparate(last, *(last.end()-1), s[0]))  {
            os << sep;
        }
        os << s;
        last = s;
    }
    std::ostream& os;
    std::string last;
    std::string sep;
};
}


std::string qMaster::QueryTemplate::dbgStr() const {
    std::stringstream ss;
    SpacedOutput so(ss, " ");
//    std::copy(_elements.begin(), _elements.end(),
//              std::ostream_iterator<std::string>(ss, ""));
    std::for_each(_elements.begin(), _elements.end(), so);
    return ss.str();
}
