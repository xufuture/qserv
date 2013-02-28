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
#ifndef LSST_QSERV_WORKER_XRDNAME_H
#define LSST_QSERV_WORKER_XRDNAME_H
#include <string>
namespace lsst { namespace qserv { namespace worker {

class XrdName {
public:
    XrdName() {
        std::string name;
        char const* xrdname = ::getenv("XRDNAME");
        if(xrdname) {
            _name = _sanitize(xrdname);
        } else {
            _name = "unknown";
        }
    }
    std::string const& getName() const { return _name; }
private:
    std::string _sanitize(char const* rawName) {
        // Sanitize xrdname
        char* sanitized = strdup(rawName);
        for(char* cursor = sanitized; *rawName != '\0'; ++rawName) {
            if(isalnum(*rawName)) { 
                *cursor = *rawName; 
                ++cursor;
            }
            *cursor = '\0';// Keep the tail null-terminated.
        }
        return sanitized;
    }
    std::string _name;
};
}}} // namespace lsst::qserv::worker

#endif // LSST_QSERV_WORKER_XRDNAME_H
