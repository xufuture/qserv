// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013-2014 LSST Corporation.
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
#ifndef LSST_QSERV_QUERY_QUERYCONTEXT_H
#define LSST_QSERV_QUERY_QUERYCONTEXT_H
/**
  * @file
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <list>
#include <string>

// Third-party headers
#include <boost/shared_ptr.hpp>

// Local headers
#include "css/Facade.h"
#include "qana/QueryMapping.h"
#include "query/Constraint.h"
#include "query/DbTablePair.h"
#include "query/QsRestrictor.h"
#include "query/TableAlias.h"
#include "util/common.h"

namespace lsst {
namespace qserv {
namespace query {

class ColumnRef;

class QueryContextRestrictors {
// Accessors
public:
    bool hasRestrictors() const {
        return _restrictors != 0;
    }
    int nRestrictors() const {
        return _restrictors->size();
    }
    const QsRestrictor& firstRestrictor() const {
        assert(nRestrictors() > 0);
        return *_restrictors->front();
    }
    boost::shared_ptr<ConstraintVector> getConstraints() const;

// Modifiers
    void resetRestrictors() {
        _restrictors.reset(new RestrList);
    }
    void resetRestrictorsIfEmpty() {
        if (_restrictors && _restrictors->empty()) {
            resetRestrictors();
        }
    }
    void addRestrictor(boost::shared_ptr<QsRestrictor> r) {
        _restrictors->push_back(r);
    }
    void mergeInRestrictors(boost::shared_ptr<QsRestrictor::List> keyPreds);

// Private type defs and class members
private:
    typedef std::list<boost::shared_ptr<QsRestrictor> > RestrList;

    boost::shared_ptr<RestrList> _restrictors;
};

class QueryContextScanTables {
// Modifiers
public:
    void setScanTables(util::StringPairList scanTables) {
        _scanTables = scanTables;
    }
    void clearScanTables() {
        _scanTables.clear();
    }

// Accessors
    void printScanTables(std::ostream& os) const {
        if(!_scanTables.empty()) {
            util::StringPairList::const_iterator i,e;
            for(i=_scanTables.begin(), e=_scanTables.end(); i != e; ++i) {
                os << "ScanTable: " << i->first << "." << i->second
                   << std::endl;
            }
        }
    }

// Private type defs and class members
public: // This should private (work in progress)
    util::StringPairList _scanTables; /// Tables scanned (for shared scans)
};
    
        
/// QueryContext is a value container for query state related to analyzing,
/// rewriting, and generating queries. It is the primary mechanism for
/// QueryPlugin instances to share information. It contains the user context of
/// a query, but not the query itself.
///
/// TODO: Think about QueryMapping's home. It is used during query manipulation,
/// contains information derived during analysis, and is used to generate
/// materialized query text.
class QueryContext 
    : public QueryContextRestrictors, 
      public QueryContextScanTables {
public:
    typedef boost::shared_ptr<QueryContext> Ptr;

    // This is used by TestFactory only, consider changing TestFactory 
    // so that we can use the other constructor
    QueryContext(std::string const& defaultDb) 
        : _defaultDb(defaultDb),
          _username("default"),
          _chunkCount(0),
          _needsMerge(false) {
    }
    QueryContext(std::string const& defaultDb, 
                 boost::shared_ptr<css::Facade> cssFacade) 
        : _cssFacade(cssFacade),
          _defaultDb(defaultDb),
          _username("default"),
          _chunkCount(0),
          _needsMerge(false) {
    }
// Accessors
    std::string const& defaultDb() const {
        return _defaultDb;
    }
    std::string const& dominantDb() const {
        return _dominantDb;
    }
    std::string const& anonymousTable() const {
        return _anonymousTable;
    }
    int chunkCount() const {
        return _chunkCount;
    }
    bool needsMerge() const {
        return _needsMerge;
    }
    lsst::qserv::css::StripingParams getDbStriping() const {
        return _cssFacade->getDbStriping(dominantDb());
    }
    bool containsDb(std::string const& dbName) const {
        return _cssFacade->containsDb(dbName);
    }
    bool hasChunks() const {
        return _queryMapping.get() && _queryMapping->hasChunks();
    }
    bool hasSubChunks() const {
        return _queryMapping.get() && _queryMapping->hasSubChunks();
    }
    
// Modifiers
    void setAnonymousTable(std::string const& t) {
        _anonymousTable = t;
    }
    void setDominantDb(std::string const& d) {
        _dominantDb = d;
    }
    void setUsername(std::string const& u) {
        _username = u;
    }
    void setNeedsMerge() {
        _needsMerge = true;
    }
    void incrChunkCount() {
        ++_chunkCount;
    }
    // This section is for resolvers, consider moving logic for resolvers
    // to a separate class
    void swapResolverTables(std::vector<DbTablePair> newRT) {
        _resolverTables.swap(newRT);
    }
    DbTablePair resolve(boost::shared_ptr<ColumnRef> cr);

// Class members, these should be private, work in progress
    boost::shared_ptr<css::Facade> _cssFacade; ///< Unowned, assumed to be alive
                                               ///  for this lifetime.
    std::string _defaultDb; ///< User session db context
    std::string _dominantDb; ///< "dominant" database for this query
    std::string _anonymousTable; ///< Implicit table context
    std::string _username; ///< unused, but reserved.
    std::vector<DbTablePair> _resolverTables; ///< Implicit column resolution
                                    /// context. Will obsolete anonymousTable.

    // Table aliasing
    TableAlias _tableAliases;
    TableAliasReverse _tableAliasReverses;

    // Owned QueryMapping and query restrictors
    boost::shared_ptr<qana::QueryMapping> _queryMapping;

private:
    int _chunkCount; ///< -1: all, 0: none, N: #chunks
    bool _needsMerge; ///< Does this query require a merge/post-processing step?
};

}}} // namespace lsst::qserv::query

#endif // LSST_QSERV_QUERY_QUERYCONTEXT_H
