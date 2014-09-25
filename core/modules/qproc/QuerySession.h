// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2014 LSST Corporation.
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
#ifndef LSST_QSERV_QPROC_QUERYSESSION_H
#define LSST_QSERV_QPROC_QUERYSESSION_H
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <list>
#include <string>

// Third-party headers
#include <boost/iterator/iterator_facade.hpp>
#include <boost/shared_ptr.hpp>

// Local headers
#include "css/Facade.h"
#include "qana/QueryPlugin.h"
#include "qproc/ChunkQuerySpec.h"
#include "qproc/ChunkSpec.h"
#include "query/Constraint.h"
#include "rproc/mergeTypes.h"


// Forward declarations
namespace lsst {
namespace qserv {
namespace css {
    class StripingParams;
}
namespace query {
    class SelectStmt;
    class QueryContext;
}}} // End of forward declarations


namespace lsst {
namespace qserv {
namespace qproc {

///  QuerySession contains state and behavior for operating on user queries. It
///  contains much of the query analysis-side of AsyncQueryManager's
///  responsibility, including the text of the original query, a parsed query
///  tree, and other user state/context.
class QuerySession {
public:
    class Iter;
    friend class Iter;
    friend class AsyncQueryManager; // factory for QuerySession.
    typedef boost::shared_ptr<QuerySession> Ptr;

    explicit QuerySession(boost::shared_ptr<css::Facade>);

    // Const accessors
    std::string const& getOriginal() const { return _original; }
    void setDefaultDb(std::string const& db);
    void setQuery(std::string const& q);
    bool hasAggregate() const;
    bool hasChunks() const;

    // Constraint exchange functions: Caller exports query spatial constraints
    // using getConstraints() after analysis, then performs geometry lookup to
    // determine chunk coverage, then pushes chunk coverage in using addChunk.
    boost::shared_ptr<query::ConstraintVector> getConstraints() const;
    void addChunk(ChunkSpec const& cs);

    query::SelectStmt const& getStmt() const { return *_stmt; }

    void setResultTable(std::string const& resultTable);
    std::string const& getResultTable() const { return _resultTable; }

    /// Dominant database is the database that will be used for query
    /// dispatch. This is distinct from the default database, which is what is
    /// used for unqualified table and column references
    std::string const& getDominantDb() const;
    bool containsDb(std::string const& dbName) const;
    css::StripingParams getDbStriping();
    std::string const& getError() const { return _error; }

    rproc::MergeFixup makeMergeFixup() const; //< as obsolete as TableMerger
    boost::shared_ptr<query::SelectStmt> getMergeStmt() const;

    /// Finalize a query after chunk coverage has been updated
    void finalize();
    // Iteration
    Iter cQueryBegin();
    Iter cQueryEnd();

    // For test harnesses.
    struct Test {
        int cfgNum;
        boost::shared_ptr<css::Facade> cssFacade;
        std::string defaultDb;
    };
    explicit QuerySession(Test& t) //< Debug constructor
    boost::shared_ptr<query::QueryContext> dbgGetContext() { return _context; }

private:
    typedef std::list<qana::QueryPlugin::Ptr> PluginList;

    // Pipeline helpers
    void _initContext();
    void _preparePlugins();
    void _applyLogicPlugins();
    void _generateConcrete();
    void _applyConcretePlugins();
    void _showFinal(std::ostream& os); // Debug

    // Iterator help
    std::vector<std::string> _buildChunkQueries(ChunkSpec const& s) const;

    // Fields
    boost::shared_ptr<css::Facade> _cssFacade; //< Metadata access facade
    std::string _defaultDb; //< User db context
    std::string _original; //< Original user query
    boost::shared_ptr<query::QueryContext> _context; //< Analysis context
    boost::shared_ptr<query::SelectStmt> _stmt; //< Logical query statement
    /// Group of parallel statements (not a sequence)
    std::list<boost::shared_ptr<query::SelectStmt> > _stmtParallel;
    boost::shared_ptr<query::SelectStmt> _stmtMerge; //< Aggregating statement
    bool _hasMerge; //< Is a merge/aggregation statement required
    std::string _tmpTable; //< Intermediate temp table
    std::string _resultTable; //< Final result table
    std::string _error; //< Error description
    int _isFinal; //< Has query analysis/optimization completed?

    ChunkSpecList _chunks; //< Chunk coverage
    boost::shared_ptr<PluginList> _plugins; //< Analysis plugin chain
};

/// Iterates over a ChunkSpecList to return ChunkQuerySpecs for execution
class QuerySession::Iter : public boost::iterator_facade <
    QuerySession::Iter, ChunkQuerySpec, boost::forward_traversal_tag> {
public:
    Iter() : _qs(NULL) {}

private:
    Iter(QuerySession& qs, ChunkSpecList::iterator i);
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
    boost::shared_ptr<ChunkQuerySpec> _buildFragment(ChunkSpecFragmenter& f) const;

    QuerySession* _qs; //< Associated QuerySession
    ChunkSpecList::const_iterator _pos; //< Iterator position
    bool _hasChunks; //< Chunked query?
    bool _hasSubChunks; //< Subchunks needed?
    mutable ChunkQuerySpec _cache; //< Query generation cache
    mutable bool _dirty; //< Does cache need updating/refreshing?
};

}}} // namespace lsst::qserv::qproc

#endif // LSST_QSERV_QPROC_QUERYSESSION_H
