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

// System headers
#include <map>

// Third-party headers
#include "log/Logger.h"
#include "parser/parserBase.h"
#include "parser/parseTreeUtil.h"


namespace lsst {
namespace qserv {
namespace parser {

class ColumnHandler : public VoidFourRefFunc {
public:
    virtual ~ColumnHandler() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b,
                            antlr::RefAST c, antlr::RefAST d) {
#ifdef NEWLOG
        LOGF_INFO("col _%1%_ _%2%_ _%3%_ _%4%_" % tokenText(a)
                  % tokenText(b) % tokenText(c) % tokenText(d));
#else
        LOGGER_INF << "col _" << tokenText(a)
                   << "_ _" << tokenText(b)
                   << "_ _" << tokenText(c)
                   << "_ _" << tokenText(d)
                   << "_ ";
#endif
        a->setText("AWESOMECOLUMN");
    }
};

class TableHandler : public VoidThreeRefFunc {
public:
    virtual ~TableHandler() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b,
                            antlr::RefAST c)  {
#ifdef NEWLOG
        LOGF_INFO("qualname %1% %2% %3% " % tokenText(a)
                  % tokenText(b) % tokenText(c));
#else
        LOGGER_INF << "qualname " << tokenText(a)
                   << " " << tokenText(b) << " "
                   << tokenText(c) << " ";
#endif
        a->setText("AwesomeTable");
    }
};

class TestAliasHandler : public VoidTwoRefFunc {
public:
    virtual ~TestAliasHandler() {}
    virtual void operator()(antlr::RefAST a, antlr::RefAST b)  {
        if(b.get()) {
#ifdef NEWLOG
            LOGF_INFO("Alias %1% = %2%" % tokenText(a) % tokenText(b));
#else
            LOGGER_INF << "Alias " << tokenText(a)
                       << " = " << tokenText(b) << std::endl;
#endif
        }
    }
};

class TestSelectListHandler : public VoidOneRefFunc {
public:
    virtual ~TestSelectListHandler() {}
    virtual void operator()(antlr::RefAST a) {
        antlr::RefAST bound = parser::getLastSibling(a);
#ifdef NEWLOG
        LOGF_INFO("SelectList %1%--From %2% to %3%"
                  % walkTreeString(a) % a % bound);
#else
        LOGGER_INF << "SelectList " << walkTreeString(a)
                   << "--From " << a << " to " << bound << std::endl;
#endif
    }
};

class TestSetFuncHandler : public VoidOneRefFunc {
public:
    typedef std::map<std::string, int> Map;
    typedef Map::const_iterator MapConstIter;
    typedef Map::iterator MapIter;

    TestSetFuncHandler() {
        _map["count"] = 1;
        _map["avg"] = 1;
        _map["max"] = 1;
        _map["min"] = 1;
        _map["sum"] = 1;
    }
    virtual ~TestSetFuncHandler() {}
    virtual void operator()(antlr::RefAST a) {
#ifdef NEWLOG
        LOGF_INFO("Got setfunc %1%" % walkTreeString(a));
#else
        LOGGER_INF << "Got setfunc " << walkTreeString(a)
                   << std::endl;
#endif
        //verify aggregation cmd.
        std::string origAgg = tokenText(a);
        MapConstIter i = _map.find(origAgg); // case-sensitivity?
        if(i == _map.end()) {
#ifdef NEWLOG
            LOGF_INFO("%1% is not an aggregate." % origAgg);
#else
            LOGGER_INF << origAgg << " is not an aggregate." << std::endl;
#endif
            return; // Skip.  Actually, this would be a parser bug.
        }
        // Extract meaning and label parts.
        // meaning is function + arguments
        // label is aliased name, if available, or function+arguments text otherwise.
        //std::string label =
    }
    Map _map;
};

}}} // namespace lsst::qserv::parser
