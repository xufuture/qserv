// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
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

#ifndef LSST_QSERV_QANA_RELATIONGRAPH_H
#define LSST_QSERV_QANA_RELATIONGRAPH_H

/// \file
/// \brief A data structure used for parallel query validation and rewriting.

/// \page par_query_rewriting Parallel Query Validation and Rewriting
///
/// [TOC]
///
/// As a consequence of its shared-nothing nature, there are limits on the
/// types of queries that Qserv can evaluate. In particular, any query
/// involving partitioned tables must be analyzed to make sure that it can
/// be decomposed into per-partition queries that are evaluable using only
/// data from that partition (on worker MySQL instances), plus a global
/// aggregation/merge step (on a czar MySQL instance). In the description
/// below, we focus on the validation and rewriting strategy for generating
/// parallel (worker-side) queries, and ignore the merge/aggregation step
/// that happens on the czar.
///
/// Join Types
/// ----------
///
/// Broadly speaking, Qserv supports equi-joins between director and match
/// or child tables, and near-neighbor spatial joins between director tables.
/// Please see the [table types](\ref table_types) page for descriptions of
/// the different kinds of tables Qserv supports.
///
/// Director-child Equi-joins
/// -------------------------
///
/// Equi-joins between director and child tables are easy to evaluate because
/// matching rows will always fall into the same chunk and sub-chunk. This
/// means that evaluating such a query in parallel over N (sub-)chunks is just
/// a matter of issuing the original query on each (sub-)chunk after replacing
/// the original table names with (sub-)chunk table names. Left and right
/// outer joins are easily supported in the same way.
///
/// Near-neighbor Joins
/// -------------------
///
/// Near-neighbor joins are harder to deal with because partition overlap must
/// be utilized. Qserv's evaluation strategy is best illustrated by means of
/// an example:
///
///     SELECT a.*, b.*
///         FROM Object AS a, Object AS b
///         WHERE scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///               a.objectId != b.objectId;
///
/// The naive evaluation strategy for this join is to consider all pairs of
/// rows (in this case, astronomical objects) and only retain those with
/// sky-positions separated by less than 0.001 degrees. We improve on this
/// wasteful O(N²) strategy by running the following pair of queries for each
/// sub-chunk of each chunk and taking the union of the results:
///
///     SELECT a.*, b.*
///         FROM Object_%CC%_%SS% AS a, Object_%CC%_%SS% AS b
///         WHERE scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///               a.objectId != b.objectId;
///     SELECT a.*, b.*
///         FROM Object_%CC%_%SS% AS a, ObjectFullOverlap_%CC%_%SS% AS b
///         WHERE scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///               a.objectId != b.objectId;
///
/// In the above, `%CC%` and `%SS%` are placeholders for a chunk and sub-chunk
/// numbers. This is O(kN), where k is the number of objects per partition,
/// and can be evaluated under the constraints of Qserv's shared-nothing model
/// so long as an overlap sub-chunk contains all objects within 0.001 degrees
/// of the corresponding sub-chunk boundary.
///
/// Clearly, k should be kept small to avoid quadratic blowup. But making
/// it too small leads to excessive query dispatch and issue overhead. This
/// is the raison d'être for sub-chunks: using them allows us to lower k
/// without having to deal with dispatching a crippling number of chunk
/// queries to workers. In practice, sub-chunk tables are not materialized
/// on-disk, but are created by workers on the fly from chunk tables using
/// `CREATE TABLE ... ENGINE=MEMORY AS SELECT`.
///
/// Notice that query rewriting is still just a matter of duplicating the
/// original query and replacing table names with sub-chunk specific names.
/// Also, there are actually two ways to decompose the query. The decomposition
/// above finds all matches for a sub-chunk of `a`, but we can instead find
/// all matches for a sub-chunk of `b`:
///
///     SELECT a.*, b.*
///         FROM Object_%CC%_%SS% AS a, Object_%CC%_%SS% AS b
///         WHERE scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///               a.objectId != b.objectId;
///     SELECT a.*, b.*
///         FROM ObjectFullOverlap_%CC%_%SS% AS a, Object_%CC%_%SS% AS b
///         WHERE scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///               a.objectId != b.objectId;
///
/// Finally, the example could just as easily have used an INNER JOIN with
/// an ON clause, instead of the abbreviated JOIN syntax and WHERE clause.
///
/// What of outer joins? FULL OUTER JOIN is not supported by MySQL, so that
/// leaves the question of what to do with:
///
///     SELECT a.*, b.*
///         FROM Object AS a LEFT OUTER JOIN
///              Object AS b ON (
///                  scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///                  a.objectId != b.objectId);
///
/// This is not evaluable using the strategy described thus far, because
/// the sub-chunk overlap is in a separate table from the sub-chunk. Instead,
/// we would have to issue the following per sub-chunk:
///
///     SELECT a.*, b.*
///         FROM Object_%CC%_%SS% AS a LEFT OUTER JOIN
///              (SELECT * FROM Object_%CC%_%SS% UNION ALL
///               SELECT * FROM ObjectFullOverlap_%CC%_%SS%) AS b ON (
///                  scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///                  a.objectId != b.objectId);
///
/// Implementing this is somewhat painful and would require changes to the
/// query IR classes. Sub-chunk generation could be changed to generate the
/// UNION above directly (rather than the `FullOverlap` tables), but this
/// approach can almost double the memory required to hold an entire chunk
/// of sub-chunks in RAM. Since the worker wants chunks to fit entirely
/// in memory (so that disk I/O for table scans can be shared across multiple
/// queries), this may not be an option. Another possibility is to only
/// generate sub-chunk tables containing both sub-chunk and overlap rows,
/// along with a flag indicating whether rows belong to the overlap region.
/// This halves the number of in-memory tables that must be created and
/// populated and has identical memory requirements to the current strategy,
/// but means that flag-based duplicate removal logic must be added to many
/// queries. RIGHT joins have the same problem, as they are equivalent to
/// LEFT joins after commuting the left and right table references.
///
/// For now, Qserv does not support LEFT or RIGHT joins with near-neighbor
/// predicates.
///
/// Match Table Joins
/// -----------------
///
/// Match table equi-joins are also complicated by overlap. If a match-table
/// is joined against only one of the director tables it matches together, the
/// situation is simple:
///
///     SELECT d1.*, m.*
///         FROM Director1 AS d1 JOIN
///              Match AS m ON (d1.id = m.id1);
///
/// can be executed by rewriting table references as before:
///
///     SELECT d1.*, m.*
///         FROM Director1_%CC% AS d1 JOIN
///              Match_%CC% AS m ON (d1.id = m.id1);
///
/// since a match to a director table row from chunk C is guaranteed to
/// lie in chunk C of the match table. Note that the query can be
/// parallelized either over director table chunks or sub-chunks. However,
/// if the join involves both director tables:
///
///     SELECT d1.*, m.*, d2.*
///         FROM Director1 AS d1 JOIN
///              Match AS m ON (d1.id = m.id1) JOIN
///              Director2 AS d2 ON (m.id2 = d2.id);
///
/// then, since it is possible for rows in `d2` to match rows in `d1` from a
/// different chunk, overlap must be used:
///
///     SELECT d1.*, m.*, d2.*
///         FROM Director1_%CC%_%SS% AS d1 JOIN
///              Match_%CC% AS m ON (d1.id = m.id1) JOIN
///              Director2_%CC%_%SS% AS d2 ON (m.id2 = d2.id);
///     SELECT d1.*, m.*, d2.*
///         FROM Director1_%CC%_%SS% AS d1 JOIN
///              Match_%CC% AS m ON (d1.id = m.id1) JOIN
///              Director2FullOverlap_%CC%_%SS% AS d2 ON (m.id2 = d2.id);
///
/// Note that while sub-chunking could be enabled for match table chunks as
/// well, doing so would increase match table storage costs since matches
/// between different sub-chunks (rather than chunks) would have to be stored
/// twice. Furthermore, it would require additional in-memory tables to be
/// created and populated, and those tables would not come with prebuilt
/// indexes on their foreign keys.
///
/// As in the near-neighbor case, there are 2-ways to decompose the query:
/// overlap from either `d1` or `d2` can be utilized. And again, because the
/// union of overlap and non-overlap results is not performed within a single
/// query, Qserv cannot support arbitrary outer equi-joins between match and
/// director tables - LEFT and RIGHT joins involving match tables are not
/// supported. Additionally, match → match table joins are not currently
/// allowed.
///
/// Query Validation Algorithm
/// --------------------------
///
/// The query validation algorithm operates by first building an undirected
/// graph from the input query, with vertices corresponding to partitioned
/// table references and edges corresponding to those join predicates that
/// can be used to make inferences about the partition of results from one
/// table based on the partition of results from another. For example, the
/// graph for the following query:
///
///     SELECT * FROM Object AS o INNER JOIN
///                   Source AS s ON (o.objectId = s.objectId);
///
/// would contain two vertices, one for Object (a director table) and one
/// for Source (a child table). The equi-join predicate forces matching
/// Object and Source rows to have the same partition, so the graph
/// has a single edge between the Object and Source vertices.
///
/// The core idea behind the validation algorithm is as follows: first pick
/// a table reference for which no overlap will be used. (Note that if there
/// are any references to partitioned tables in the query, then we must
/// refrain from using overlap for at least one of them to avoid duplicate
/// result rows.) Then, use the graph to infer that rows from all other table
/// references have the same partition as the rows from the initial reference,
/// or fall within its overlap. If this is possible, the input query is
/// evaluable.
///
/// Note that if the graph G is not connected, we will never be able to infer
/// locality for all table references, no matter which graph vertex (table
/// reference) we start from. In other words, Qserv must assume that it cannot
/// evaluate the query using only worker-local data and report an error back
/// to the user.
///
/// While the connectedness of G is a necessary condition for query
/// evaluability, it is not sufficient. Further analysis is required because
/// some join predicates (spatial predicates, equi-join predicates for match
/// tables) require the presence of overlap that may not be available. For
/// example, a query that equi-joins a child C₁ of director D₁ to a match
/// table M and then to child C₂ of director D₂ would require overlap for
/// either C₁ or C₂. Since overlap is not stored for child tables, the query
/// is not evaluable, even though the corresponding graph is connected. In
/// addition, if a query references one or more director tables, then one must
/// determine the directors for which overlap is required. These problems are
/// tackled by performing what is essentially a series of graph traversals:
///
/// 1. Let S be the set of vertices corresponding to child or director tables.
///
/// 2. Given a vertex v ∈ S, assume that the corresponding rows are strictly
///    within a partition; that is, the overlap oᵥ required for v is 0. Set
///    the required overlap for all other vertices to ∞, and create an empty
///    vertex queue Q.
///
/// 3. For each edge e incident to vertex v, infer the overlap oᵤ required
///    for vertex u reachable from v via e. If oᵤ is greater than the
///    available overlap, ignore u. Otherwise, set the required overlap for
///    u to the minimum of oᵤ and its current required overlap. If oᵤ was
///    smaller than the previous required overlap and u is not already in Q,
///    insert u into Q. oᵤ is determined from oᵥ based on the kinds of tables
///    linked by e (v → u):
///
///    - director → director:
///      oᵤ = oᵥ for an equi-join edge
///      oᵤ = oᵥ + a for a spatial edge with angular separation threshold a.
///
///    - match → match:
///      oᵤ = oᵥ + p, where p is the partition overlap
///
///    - all others edges:
///      oᵤ = oᵥ
///
///    Note: match table references are represented internally as a pair of
///    vertices connected by a spatial match → match edge with angular
///    separation threshold equal to the partition overlap. This is the only
///    way match → match edges can be created. Each vertex in the pair is
///    assigned the column references for one of the director table foreign
///    keys.
///
/// 4. Remove a vertex from Q and repeat step 3 until there are no more
///    vertices left to process in Q.
///
/// 5. If no vertex has a required overlap of ∞ after Q has been emptied, then
///    the query is evaluable; the directors requiring overlap will have been
///    identified by the graph traversal above. Otherwise, choose another
///    vertex from S, and repeat the process starting at step 2.
///
/// 6. If all graph traversals starting from vertices in S result in one or
///    more vertices having a required overlap of ∞, then the query is not
///    evaluable by Qserv.
///
/// Query Rewriting
/// ---------------
///
/// As alluded to earlier, the current query rewriting strategy involves
/// copying the input query and replacing the table references in its FROM
/// clause with chunk and sub-chunk specific table name patterns. The result
/// is a set of query templates into which specific (sub-)chunk numbers can
/// be substituted to obtain the actual queries that run on Qserv workers.
///
/// If the input query does not require overlap for any directors, then the
/// task is simple - we replace all partitioned table-references with
/// chunk-specific table name patterns. The input query is rewritten to a
/// single output query template.
///
/// If overlap is required for one or more directors things are still
/// relatively simple conceptually. Recall that overlap is stored in a
/// separate in-memory table per sub-chunk. Given an input query
/// that looks like:
///
///     SELECT * FROM D1, D2, ... Dn ...;
///
/// where D1, D2, ... Dn are the directors requiring overlap, the rewriting
/// must produce the same results as:
///
///     SELECT * FROM
///         (SELECT * FROM D1_%CC%_%SS% UNION ALL SELECT * D1FullOverlap_%CC%_%SS%),
///         (SELECT * FROM D2_%CC%_%SS% UNION ALL SELECT * D2FullOverlap_%CC%_%SS%),
///         ...
///         (SELECT * FROM Dn_%CC%_%SS% UNION ALL SELECT * DnFullOverlap_%CC%_%SS%)
///     ...;
///
/// Unfortunately, the current IR class design does not allow that specific
/// rewriting due to lack of subquery support. However:
///
///     SELECT * FROM (SELECT * FROM A₀ UNION ALL SELECT * FROM A₁), B, ...;
///
/// is equivalent to the union of the results of the following pair of queries
/// in the absence of aggregation and sorting:
///
///     (SELECT * FROM A₀, B, ...);
///     (SELECT * FROM A₁, B, ...);
///
/// Applying the same rule twice allows us to transform:
///
///     SELECT ... FROM (SELECT * FROM A₀ UNION ALL SELECT * FROM A₁),
///                     (SELECT * FROM B₀ UNION ALL SELECT * FROM B₁), ...;
///
/// to a union of the following 4 queries:
///
///     (SELECT * FROM A₀, B₀, ...);
///     (SELECT * FROM A₀, B₁, ...);
///     (SELECT * FROM A₁, B₀, ...);
///     (SELECT * FROM A₁, B₁, ...);
///
/// In our case, the deferral of aggregation/sorting to the merge step on the
/// czar in conjunction with the join limitations discussed earlier allow us
/// to apply the same transformation in general, not just for the cross joins
/// illustrated above. So an input query containing N union-pair sub-queries
/// can be transformed to a union of 2ᴺ queries without such sub-queries.
///
/// The actual rewriting is performed by assigning a bit to each of the N
/// directors requiring overlap. A bit value of 0 is taken to mean that a
/// director table reference should be replaced with a sub-chunk specific
/// table name pattern. A value of 1 means it should be replaced with an
/// overlap sub-chunk table name pattern instead. Concatenating these bits
/// yields an N-bit integer where each possible value (0, 1, ..., 2ᴺ-1)
/// specifies the table reference substitutions required to obtain a single
/// output query template.
///
/// Because the time and space complexity of our query rewriting/execution
/// strategy is exponential in the number of table references requiring
/// overlap, we impose a strict limit on the maximum number of such
/// references.

// System headers
#include <limits>
#include <list>
#include <string>
#include <vector>

// Third-party headers
#include <boost/math/special_functions/fpclassify.hpp> // for isnan, isinf
#include <boost/shared_ptr.hpp>

// Local headers
#include "ColumnVertexMap.h"
#include "TableInfo.h"

#include "query/BoolTerm.h"
#include "query/JoinRef.h"
#include "query/JoinSpec.h"
#include "query/TableRef.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace query {
    class ColumnRef;
    class QueryContext;
    class SelectStmt;
}
namespace qana {
    class QueryMapping;
    class TableInfoPool;
}}}


namespace lsst {
namespace qserv {
namespace qana {

typedef std::list<boost::shared_ptr<query::SelectStmt> > SelectStmtList;

struct Vertex;

/// An `Edge` is a minimal representation of an admissible join predicate.
/// An admissible join predicate is one that can be used to infer the
/// the partition of rows in one table from the partition of rows in
/// another.
///
/// An edge corresponds to an equi-join predicate iff `angSep` is NaN.
/// Otherwise, it corresponds to a spatial predicate that constrains the
/// angle between two spherical coordinate pairs to be less than or equal to
/// `angSep`.
///
/// Note that the names of the columns involved in a predicate can be obtained
/// by examining the table references that are linked by its edge; for any
/// pair of references there is at most one equi-join and one spatial predicate
/// that can link them. Only one of the edge vertices is stored; the other owns
/// the `Edge` and is therefore implicitly available.
struct Edge {
    enum Classification {
        DIRECTOR_DIRECTOR = 0,
        DIRECTOR_CHILD,
        DIRECTOR_MATCH,
        CHILD_DIRECTOR,
        CHILD_CHILD,
        CHILD_MATCH,
        MATCH_DIRECTOR,
        MATCH_CHILD,
        MATCH_MATCH
    };

    Vertex* vertex; // unowned
    double angSep;

    Edge() : vertex(0), angSep(0.0) {}
    Edge(Vertex* v, double a) : vertex(v), angSep(a) {}

    bool isSpatial() const { return !(boost::math::isnan)(angSep); }
    bool operator<(Edge const& p) const { return vertex < p.vertex; }
    bool operator==(Edge const& p) const { return vertex == p.vertex; }
    bool operator!=(Edge const& p) const { return vertex != p.vertex; }

    static inline Classification classify(Vertex const& from, Vertex const& to);
};


/// A `Vertex` corresponds to an in-query partitioned table reference. A
/// reference to the underlying table metadata and a list of edges (join
/// predicates/constraints) that involve the table reference are bundled
/// alongside.
struct Vertex {
    query::TableRef& tr;
    TableInfo const* info;
    Vertex* next;
    double overlap;
    std::vector<Edge> edges; // sorted

    Vertex(query::TableRef& t, TableInfo const* i) :
        tr(t),
        info(i),
        next(0),
        overlap(std::numeric_limits<double>::infinity())
    {}

    /// `insert` adds the given join predicate to the set of predicates
    /// involving this table reference. Inserting a duplicate of an existing
    /// predicate has no effect.
    void insert(Edge const& e);

    /// `rewriteAsChunkTemplate` rewrites `tr` to contain a chunk specific
    /// name pattern.
    void rewriteAsChunkTemplate() {
        tr.setDb(info->database);
        tr.setTable(info->getChunkTemplate());
    }

    /// `rewriteAsSubChunkTemplate` rewrites `tr` to contain a sub-chunk
    /// specific name pattern.
    void rewriteAsSubChunkTemplate() {
        tr.setDb(info->getSubChunkDb());
        tr.setTable(info->getSubChunkTemplate());
    }

    /// `rewriteAsOverlapTemplate` rewrites `tr` to contain an overlap
    /// sub-chunk specific name pattern.
    void rewriteAsOverlapTemplate() {
        tr.setDb(info->getSubChunkDb());
        tr.setTable(info->getOverlapTemplate());
    }
};


/// A relation graph consists of a list of vertices, representing the
/// partitioned table references of a query, linked by an edge for each join
/// predicate that can be used to infer the partition of rows in one table
/// from the partition of rows in another.
///
/// An empty relation graph represents a set of references to replicated
/// tables that are joined in some arbitrary way.
///
/// Methods provide only basic exception safety - if a problem occurs, no
/// memory is leaked, but the graph and any output parameters may be in
/// inconsistent states and should no longer be used for query analysis.
class RelationGraph {
public:
    /// The maximum number of table references in a query that can require
    /// overlap before Qserv will throw up its digital hands in protest.
    static size_t const MAX_TABLE_REFS_WITH_OVERLAP = 8;

    /// This constructor creates a relation graph from a query.
    /// If the query is not evaluable, an exception is thrown.
    RelationGraph(query::QueryContext const& ctx,
                  query::SelectStmt& stmt,
                  TableInfoPool& pool);

    /// `empty` returns `true` if this graph has no vertices.
    bool empty() const { return _vertices.empty(); }

    /// `rewrite` rewrites the input query into a set of output queries.
    void rewrite(SelectStmtList& outputs,
                 QueryMapping& mapping);

    void swap(RelationGraph& g) {
        _vertices.swap(g._vertices);
        _map.swap(g._map);
    }

private:
    std::list<Vertex> _vertices;
    ColumnVertexMap _map;
    query::SelectStmt* _query; // unowned

    // Not implemented
    RelationGraph(RelationGraph const&);
    RelationGraph& operator=(RelationGraph const&);

    // Constructors and related helpers
    RelationGraph() : _query(0) {}
    RelationGraph(query::TableRef& tr,
                  TableInfo const* info,
                  double overlap);
    RelationGraph(query::QueryContext const& ctx,
                  query::TableRef::Ptr const& tr,
                  double overlap,
                  TableInfoPool& pool);

    size_t _makeOnEqEdges(query::BoolTerm::Ptr on,
                          query::JoinRef::Type jt,
                          RelationGraph& g);
    size_t _makeNaturalEqEdges(query::JoinRef::Type jt, RelationGraph& g);
    size_t _makeUsingEqEdges(query::ColumnRef const& c,
                             query::JoinRef::Type jt,
                             RelationGraph& g);
    size_t _makeWhereEqEdges(query::BoolTerm::Ptr where);
    size_t _makeSpEdges(query::BoolTerm::Ptr term, double overlap);
    void _join(query::JoinRef::Type joinType,
               bool natural,
               query::JoinSpec::Ptr const& joinSpec,
               double overlap,
               RelationGraph& g);

    // Graph validation
    bool _validate(double overlap);
};


inline Edge::Classification Edge::classify(Vertex const& from,
                                           Vertex const& to)
{
    return static_cast<Edge::Classification>(
        from.info->kind * TableInfo::NUM_KINDS + to.info->kind);
}

}}} // namespace lsst::qserv::qana

#endif // LSST_QSERV_QANA_RELATIONGRAPH_H
