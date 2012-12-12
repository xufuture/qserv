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
// A QuerySession contains information regarding a top-level query, including
// the text of the original query, a parsed query tree, and other user
// state/context. 

#ifndef LSST_QSERV_MASTER_QUERYSESSION_H
#define LSST_QSERV_MASTER_QUERYSESSION_H
#include <list>
#include <string>
#include <boost/iterator/iterator_facade.hpp>
#include <boost/shared_ptr.hpp>

#include "lsst/qserv/master/transaction.h"
#include "lsst/qserv/master/ChunkQuerySpec.h"
#include "lsst/qserv/master/QueryPlugin.h"

namespace lsst { namespace qserv { namespace master {
class SelectStmt; // forward
class QueryPlugin; // forward

class QuerySession {
public:
    class Iter;
    friend class Iter;
    friend class AsyncQueryManager; // factory for QuerySession.

    void setQuery(std::string const& q);
    bool getHasAggregate() const;
    ConstraintVector getConstraints() const;
    void addChunk(ChunkSpec const& cs);
    
    SelectStmt const& getStmt() const { return *_stmt; }
    // Iteration
    Iter cQueryBegin();
    Iter cQueryEnd();
    
    // For test harnesses.
    struct Test { int cfgNum; };
    explicit QuerySession(Test const& t) {}
private:
    typedef std::list<ChunkSpec> ChunkSpecList;
    typedef std::list<QueryPlugin::Ptr> PluginList;

    QuerySession();

    // Pipeline helpers
    void _preparePlugins();
    void _applyLogicPlugins();
    void _generateConcrete();
    void _applyConcretePlugins();

    // Fields
    boost::shared_ptr<SelectStmt> _stmt;
    boost::shared_ptr<SelectStmt> _stmtParallel;
    boost::shared_ptr<SelectStmt> _stmtMerge;
    bool _hasMerge;
    std::string _tmpTable;
    std::string _resultTable;

    ChunkSpecList _chunks;
    boost::shared_ptr<PluginList> _plugins;
};


class QuerySession::Iter : public boost::iterator_facade <
    QuerySession::Iter, ChunkQuerySpec, boost::forward_traversal_tag> {
public:
    Iter() : _qs(NULL) {}

private:
    Iter(QuerySession& qs, ChunkSpecList::iterator i) 
        : _qs(&qs), _pos(i), _dirty(false) {}
    friend class QuerySession;
    friend class boost::iterator_core_access;

    void increment() { ++_pos; _dirty = true; }

    bool equal(Iter const& other) const {
        return (this->_qs == other._qs) && (this->_pos == other._pos);
    }

    ChunkQuerySpec& dereference() const;

    void _buildCache() const;
    void _updateCache() const {
        if(_dirty) {
            _buildCache();
            _dirty = false;
        }
    }

    QuerySession* _qs;
    ChunkSpecList::const_iterator _pos;
    mutable ChunkQuerySpec _cache;
    mutable bool _dirty;
};

void initQuerySession(); // Initialize QuerySession-related statics.
}}} // namespace lsst::qserv::master


#endif // LSST_QSERV_MASTER_QUERYSESSION_H

