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
 
#ifndef LSST_QSERV_WORKER_XROOTFILENAME_H
#define LSST_QSERV_WORKER_XROOTFILENAME_H

#include <string>
#include <map>
/* #include <regex.h> */
/* #include <assert.h> */

namespace lsst {
namespace qserv {
namespace worker {

class XrootFilename {
public:
    typedef std::map<std::string, std::string> Map;
    explicit XrootFilename(std::string const& fn) : _original(fn) {
        _setup(); }
    explicit XrootFilename(char const* fn) : _original(fn) {
        _setup(); }

    std::string getFile() const { 
        return _original.substr(0, _splitPos);
    }
    std::string getQueryString() const { 
        if(_splitPos != std::string::npos) {
            return _original.substr(_splitPos+1, std::string::npos);
        } return std::string();
    }

    bool hasKey(std::string const& k) const {
        return _map.find(k) != _map.end();
    }

    std::string getValue(std::string const& k) const {
        Map::const_iterator i = _map.find(k);
        if(i != _map.end()) {
            return i->second;
        }
        return std::string();            
    }

    void addValue(std::string const& k, std::string const& value);

private:
    typedef std::string::size_type Size;
    class TokenAcceptor;
    void _setup() {
        _findSplit();
        _makeMap(getQueryString());
    }

    void _findSplit();
    void _makeMap(std::string const& queryString); 
    void _updateString();

    template <class F>
    void _tokenize(std::string const& s, char const sep, F& f) {
        Size c = 0;
        while(c != std::string::npos) {
            Size p = s.find(sep, c);
            std::string t;
            if(p != std::string::npos) {
                t = s.substr(c, p - c);
                c = p + 1;
            } else { 
                t = s.substr(c); 
                c = p;
            }
            if(t.size() > 0) f(t);
        }
    }    

    std::string _original;
    Size _splitPos;
    Map _map;
            
};
    

}}} // namespace lsst::qserv::worker

#endif // LSST_QSERV_WORKER_XROOTFILENAME_H
