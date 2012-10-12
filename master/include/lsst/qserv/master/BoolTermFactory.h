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
// BoolTermFactory is a factory class for BoolTerm objects that get placed
// (typically) in WhereClause objects. 

#ifndef LSST_QSERV_MASTER_BOOLTERMFACTORY_H
#define LSST_QSERV_MASTER_BOOLTERMFACTORY_H

#include "lsst/qserv/master/WhereClause.h"
#include <antlr/AST.hpp>

namespace lsst { namespace qserv { namespace master {
class BoolTermFactory {
public:
    BoolTermFactory() {}
    template <class Apply, class Reject>
    class applyExcept {
    public:
        applyExcept(Apply& af, Reject& rf) : _af(af), _rf(rf)  {}
        void operator()(antlr::RefAST a) {
            if(!_rf(a)) _af(a);
        }
    private:
        Apply& _af;
        Reject& _rf;        
    };
    template <typename Term>
    class multiImport {
    public:
        multiImport(BoolTermFactory& bf, Term& t) : _bf(bf), _t(t)  {}
        void operator()(antlr::RefAST a) {
            _t._terms.push_back(_bf.newBoolTerm(a));
        }
    private:
        BoolTermFactory& _bf;
        Term& _t;        
    };
    struct tagPrint {
        tagPrint(std::ostream& os_, std::string const& tag_) 
            : os(os_), tag(tag_) {}
        void operator()(antlr::RefAST a) {
            os << tag << ": " << tokenText(a) << std::endl;
        }
        std::ostream& os;
        std::string tag;
    };
    struct spacePrint {
        spacePrint(std::ostream& os_) : os(os_), count(0) {}
        void operator()(antlr::RefAST a) {
            if(++count > 1) os << " ";
            os << tokenText(a);
        }
        std::ostream& os;
        int count;
    };


    BoolTerm::Ptr newBoolTerm(antlr::RefAST a);
    OrTerm::Ptr newOrTerm(antlr::RefAST a);
    AndTerm::Ptr newAndTerm(antlr::RefAST a);
    BoolFactor::Ptr newBoolFactor(antlr::RefAST a);
    UnknownTerm::Ptr newUnknown(antlr::RefAST a);
};

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_BOOLTERMFACTORY_H

