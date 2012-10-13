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
// WhereClause.h is a representation of a parsed SQL WHERE.

#ifndef LSST_QSERV_MASTER_WHERECLAUSE_H
#define LSST_QSERV_MASTER_WHERECLAUSE_H


// Std
#include <iostream>
#include <list>
// Boost
#include <boost/shared_ptr.hpp>
// Qserv
#include "lsst/qserv/master/ColumnRefList.h"

#if 0
#include <map>
#include <deque>
#include <antlr/AST.hpp>


#include "lsst/qserv/master/TableRefN.h"
#endif

namespace lsst { namespace qserv { namespace master {
class BoolTerm; // Forward

class QsRestrictor {
public:
    typedef boost::shared_ptr<QsRestrictor> Ptr;
    typedef std::list<Ptr> List;
    typedef std::list<std::string> StringList;

    class render {
    public:
        render(QueryTemplate& qt_) : qt(qt_) {}
        void operator()(QsRestrictor::Ptr const& p);
        QueryTemplate& qt;
    };

    std::string _name;
    StringList _params;
};

class WhereClause {
public:
    WhereClause() : _columnRefList(new ColumnRefList()) {}
    ~WhereClause() {}
    boost::shared_ptr<ColumnRefList> getColumnRefList() {
        return _columnRefList;
    }
    std::string getGenerated();

private:
    friend std::ostream& operator<<(std::ostream& os, WhereClause const& wc);
    friend class WhereFactory;

    void _resetRestrs();
    
    std::string _original;
    boost::shared_ptr<ColumnRefList> _columnRefList;
    boost::shared_ptr<BoolTerm> _tree;
    boost::shared_ptr<QsRestrictor::List> _restrs;

};

class BoolTerm {
public:
    typedef boost::shared_ptr<BoolTerm> Ptr;
    typedef std::list<Ptr> PtrList;

    virtual ~BoolTerm() {}

    friend std::ostream& operator<<(std::ostream& os, BoolTerm const& bt);
    virtual std::ostream& putStream(std::ostream& os) const = 0;

};
class OrTerm : public BoolTerm {
public:    
    typedef boost::shared_ptr<OrTerm> Ptr;
    virtual std::ostream& putStream(std::ostream& os) const;
    BoolTerm::PtrList _terms;
};
class AndTerm : public BoolTerm {
public:
    typedef boost::shared_ptr<AndTerm> Ptr;
    virtual std::ostream& putStream(std::ostream& os) const;
    BoolTerm::PtrList _terms;
};
class BfTerm {
public:
    typedef boost::shared_ptr<BfTerm> Ptr;
    typedef std::list<Ptr> PtrList;
    virtual ~BfTerm() {}
    virtual std::ostream& putStream(std::ostream& os) const = 0;
};
class BoolFactor : public BoolTerm {
public:
    typedef boost::shared_ptr<BoolFactor> Ptr;
    virtual std::ostream& putStream(std::ostream& os) const;
    BfTerm::PtrList _terms;
};
class UnknownTerm : public BoolTerm {
public:
    typedef boost::shared_ptr<UnknownTerm> Ptr;
    virtual std::ostream& putStream(std::ostream& os) const;
};
class PassTerm : public BfTerm {
public:
    typedef boost::shared_ptr<PassTerm> Ptr;
    virtual std::ostream& putStream(std::ostream& os) const;
    std::string _text;
};
class ValueExprTerm : public BfTerm {
public:
    typedef boost::shared_ptr<ValueExprTerm> Ptr;
    virtual std::ostream& putStream(std::ostream& os) const;
    boost::shared_ptr<ValueExpr> _expr;
};

}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_WHERECLAUSE_H

