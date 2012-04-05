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

#include "QservPath.hh"
#include <iostream>
#include <sstream>
namespace qsrv = lsst::qserv;

//////////////////////////////////////////////////////////////////////
// qsrv::QservPath::Tokenizer
// A simple class to tokenize paths.
//////////////////////////////////////////////////////////////////////
class qsrv::QservPath::Tokenizer {
public:
    Tokenizer(std::string s, char sep) 
        : _cursor(0), _next(0), _s(s), _sep(sep) {
        _seek();
    }

    std::string token() { return _s.substr(_cursor, _next-_cursor); }

    int tokenAsInt() {
        int num;
        std::stringstream csm(token());
        csm >> num;        
        return num;
    }

    void next() { 
        if(_next != std::string::npos) {
            _cursor = _next + 1; 
            _seek(); 
        } else { _cursor = _next; }
    }
        
    bool done() {
        return _cursor == std::string::npos;
    }
private:
    void _seek() { 
        _next = _s.find_first_of(_sep, _cursor); 
        if(_next == std::string::npos) {
            
        }
    }

    std::string::size_type _cursor;
    std::string::size_type _next;
    std::string const _s;
    char _sep;
};

//////////////////////////////////////////////////////////////////////
// qsrv::QservPath
//////////////////////////////////////////////////////////////////////
qsrv::QservPath::QservPath(std::string const& path) {
    _setFromPath(path);
}

std::string qsrv::QservPath::path() const {
    std::stringstream ss;
    ss << _pathSep << prefix(_requestType);
    if(_requestType == CQUERY) {
        ss << _pathSep << _db << _pathSep << _chunk;
    } else if(_requestType == RESULT){
        ss << _pathSep << _hashName;
        if(_vars.size() > 0) {
            ss << _varSep << _renderVars();
        }
    }
    return ss.str();
}

std::string qsrv::QservPath::var(std::string const& key) const {
    VarMap::const_iterator ci = _vars.find(key);
    if(ci != _vars.end()) {
        return ci->second;
    }
    return std::string();
}

bool qsrv::QservPath::hasVar(std::string const& key) const {
    VarMap::const_iterator ci = _vars.find(key);
    return ci != _vars.end();
}
    
std::string qsrv::QservPath::prefix(RequestType const& r) const {
    switch(r) {
    case CQUERY:
        return "q";
    case UNKNOWN:
        return "UNKNOWN";
    case OLDQ1:
        return "query";
    case OLDQ2:
        return "query2";
    case RESULT:
        return "result";
    case GARBAGE:
    default:
        return "GARBAGE";
    }
}

void qsrv::QservPath::setAsCquery(std::string const& db, int chunk) {
    _requestType = CQUERY;
    _db = db;
    _chunk = chunk;
}
void qsrv::QservPath::setAsResult(std::string const& hashName) {
    _requestType = RESULT;
    _hashName = hashName;
}


// Add optional specifiers ?foo&bar=1&bar2=2
void qsrv::QservPath::addVar(std::string const& key) {
    _vars[key]; // Insert empty entry.
}

void qsrv::QservPath::addVar(std::string const& key, int val) {
    std::stringstream ss;
    ss << val;
    _vars[key] = ss.str();
}

void qsrv::QservPath::addVar(std::string const& key, std::string const& val) {
    _vars[key] = val;
}

void qsrv::QservPath::importVarStr(std::string const& varStr) {
    Tokenizer t(varStr, _varDelim);
    for(; !t.done(); t.next()) {
        _ingestKeyStr(t.token());
    } 
}

std::ostream& operator<<(std::ostream& os, lsst::qserv::QservPath const& qp) {
    os << "QservPath:" << qp.path();
    return os;
}

//////////////////////////////////////////////////////////////////////
// qsrv::QservPath (private)
//////////////////////////////////////////////////////////////////////

void qsrv::QservPath::_setFromPath(std::string const& path) {
    std::string rTypeString;
    Tokenizer t(path, _pathSep);
    if(!t.token().empty()) { // Expect leading separator (should start with /)
        _requestType = UNKNOWN;
        return;
    }
    t.next();
    rTypeString = t.token();
    if(rTypeString == prefix(CQUERY)) {
        // Import as chunk query
        _requestType = CQUERY;
        t.next();
        _db = t.token();
        if(_db.empty()) {
            _requestType = GARBAGE;
            return;
        }
        t.next();
        _chunk = t.tokenAsInt();
    } else if(rTypeString == prefix(RESULT)) {
        _requestType = RESULT;
        t.next();
        _hashName = _ingestKeys(t.token()); 
    } else if(rTypeString == prefix(OLDQ1)) {
        _requestType = OLDQ1;
        t.next();
        _chunk = t.tokenAsInt();
    } else if(rTypeString == prefix(OLDQ2)) {
        _requestType = OLDQ2;
        t.next();
        _chunk = t.tokenAsInt();
    } else {
        _requestType = GARBAGE;
    }    
}

/// @return leaf, without ?key=value&key=value... string.
std::string qsrv::QservPath::_ingestKeys(std::string const& leafPlusKeys) {
    std::string::size_type start;
    start = leafPlusKeys.find_first_of(_varSep, 0);
    _vars.clear();

    if(start == std::string::npos) { // No keys found        
        return leafPlusKeys;
    }
    importVarStr(leafPlusKeys.substr(start+1));
    return leafPlusKeys.substr(0, start);
}

void qsrv::QservPath::_ingestKeyStr(std::string const& keyStr) {
    std::string::size_type equalsPos;
    equalsPos = keyStr.find_first_of(_eqSep);
    if(equalsPos == std::string::npos) { // No = clause, value-less key.
        _vars[keyStr] = std::string(); // empty insert.
    } else {
        _vars[keyStr.substr(0,equalsPos)] = keyStr.substr(equalsPos+1);
    }    
}

/// @return varstr: e.g. ?k=v&k=v&k=v ...
std::string qsrv::QservPath::_renderVars() const {
    std::stringstream ss;
    VarMap::const_iterator i;
    bool hasPrev = false;
    for(i=_vars.begin(); i != _vars.end(); ++i) {
        if(hasPrev) ss << _varDelim;
        ss << i->first;
        if(!i->second.empty()) {
           ss << _eqSep << i->second;
        }
        hasPrev = true;
    }
    return ss.str();
}
