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

/// \file
/// \brief A data structure used for query validation and rewriting.

/// \page query_validation_and_rewriting Query Validation and Rewriting
///
/// As a consequence of its shared-nothing nature, there are limits on the
/// types of queries that Qserv can evaluate. In particular, any query
/// involving partitioned tables must be analyzed to make sure that it can
/// be decomposed into per-partition queries that are evaluable using only
/// data from that partition (on worker MySQL instances), plus a global
/// aggregation/merge step (on a czar MySQL instance).
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
/// sub-chunk and taking the union of the results:
///
///     SELECT a.*, b.*
///         FROM Object_<partition> AS a, Object_<partition> AS b
///         WHERE scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///               a.objectId != b.objectId;
///     SELECT a.*, b.*
///         FROM Object_<partition> AS a, ObjectFullOverlap_<partition> AS b
///         WHERE scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///               a.objectId != b.objectId;
///
/// This is O(kN), where k is the number of objects per partition, and can
/// be evaluated under the constraints of Qserv's shared-nothing-with-overlap
/// model so long as the overlap partition contains all objects within 0.001
/// degrees of the partition boundary.
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
/// above finds all matches for a sub-chunk of `a`, but one can instead find
/// all matches for a sub-chunk of `b`:
///
///     SELECT a.*, b.*
///         FROM Object_<partition> AS a, Object_<partition> AS b
///         WHERE scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///               a.objectId != b.objectId;
///     SELECT a.*, b.*
///         FROM ObjectFullOverlap_<partition> AS a, Object_<partition> AS b
///         WHERE scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///               a.objectId != b.objectId;
///
/// Finally, the example is a CROSS JOIN that can easily be transformed
/// to an INNER JOIN by changing the WHERE clause to an ON clause. But what of
/// outer joins? FULL OUTER JOIN is not supported by MySQL, so that leaves
/// the question of what to do with:
///
///     SELECT a.*, b.*
///         FROM Object AS a LEFT OUTER JOIN
///              Object AS b ON (
///                  scisql_angSep(a.ra, a.dec, b.ra, b.dec) < 0.001 AND
///                  a.objectId != b.objectId);
///
/// This is not evaluable using the strategy described thus far, because
/// the sub-chunk overlap is in a separate table from the sub-chunk. Instead,
/// one would have to issue the following per sub-chunk:
///
///     SELECT a.*, b.*
///         FROM Object_<partition> AS a LEFT OUTER JOIN
///              (SELECT * FROM Object_<partition> UNION ALL
///               SELECT * FROM ObjectFullOverlap_<partition>) AS b ON (
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
/// Match table equi-joins are also complicated by the necessity to use
/// overlap. If a match-table is joined against only one of the director
/// tables it matches up with, the situation is simple:
///
///     SELECT d1.*, m.*
///         FROM Director1 AS d1 JOIN
///              Match AS m ON (d1.id = m.id1);
///
/// can be executed by rewriting table references as before:
///
///     SELECT d1.*, m.*
///         FROM Director1_<partition> AS d1 JOIN
///              Match_<chunk> AS m ON (d1.id = m.id1);
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
///         FROM Director1_<subchunk> AS d1 JOIN
///              Match_<chunk> AS m ON (d1.id = m.id1) JOIN
///              Director2_<subchunk> AS d2 ON (m.id2 = d2.id);
///     SELECT d1.*, m.*, d2.*
///         FROM Director1_<subchunk> AS d1 JOIN
///              Match_<chunk> AS m ON (d1.id = m.id1) JOIN
///              Director2FullOverlap_<subchunk> AS d2 ON (m.id2 = d2.id);
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
/// director tables. So again, Qserv does not support LEFT or RIGHT joins
/// involving match tables. Additionally, match → match table joins are
/// not currently allowed.
///
/// Query Validation Algorithm
/// --------------------------
///
/// Admissible join predicates constrain the rows from two table references
/// to be in the same partition (or close by). At a high level, the query
/// validation algorithm operates by first building an undirected graph with
/// vertices corresponding to partitioned table references and edges
/// corresponding to those join predicates that can be used to make inferences
/// about the partition of results from one table based on the partition of
/// results from another.
///
/// If this graph G is not connected, then there are two subgraphs G₁ and G₂
/// such that localizing the rows of table references in G₁ to be within a
/// single partition and its overlap does not imply that the corresponding
/// rows in G₂ share that partition. In other words, Qserv must assume that
/// it cannot evaluate the query using only worker-local data.
///
/// The connectedness of G is a necessary condition for query evaluability,
/// but it is not sufficient. Further analysis is required because some join
/// predicates (spatial predicates, equi-join predicates for match tables)
/// require the presence of overlap that may not be available. For example,
/// a query that equi-joins a child C₁ of director D₁ to a match table M and
/// then to child C₂ of director D₂ would require overlap for either C₁ or C₂.
/// Since overlap is not stored for child tables, the query is not evaluable
/// even though the corresponding relation graph is connected.
///
/// In addition, if a query references one or more director tables, then
/// one must determine the directors for which overlap is required. This is
/// done using a series of graph traversals:
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
///    partition overlap, ignore u. Otherwise, set the required overlap for
///    u to the minimum of oᵤ and its current required overlap. If oᵤ was
///    smaller than the previous required overlap and u is not already in Q,
///    insert u into Q.
///
///    Required overlap inference occurs based on edge classification as
///    follows:
///
///    - director → director:
///        oᵤ = oᵥ for an equi-join edge
///        oᵤ = oᵥ + a for a spatial edge with angle threshold a.
///
///    - match → match:
///        oᵤ = oᵥ + p, where p is the partition overlap
///
///    - all others edges:
///        oᵤ = oᵥ
///
///    Note: match table references are represented internally as a pair of
///    vertices connected by a spatial match → match edge with angular
///    separation threshold equal to the partition overlap. Each vertex is
///    assigned the column references for one of the director table foreign
///    keys.
///
/// 4. Remove a vertex from Q and repeat step 3 until there are no more
///    vertices left to process.
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
/// FIXME: describe this.

#ifndef LSST_QSERV_QANA_RELATIONGRAPH_H
#define LSST_QSERV_QANA_RELATIONGRAPH_H

// System headers
#include <limits>
#include <string>
#include <vector>

// Third-party headers
#include <boost/math/special_functions/fpclassify.hpp> // for isnan, isinf

// Local headers
#include "ColumnVertexMap.h"
#include "TableInfo.h"
#include "TableInfoPool.h"

#include "query/BoolTerm.h"
#include "query/ColumnRef.h"
#include "query/JoinRef.h"
#include "query/JoinSpec.h"
#include "query/QueryContext.h"
#include "query/SelectStmt.h"
#include "query/TableRef.h"


namespace lsst {
namespace qserv {
namespace qana {

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
/// by examining the table references that are linked by the predicate. Only
/// one of these is explicitly stored; the other owns the `Edge` and is
/// therefore implicitly available.
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
/// memory is leaked, but the graph may be in an inconsistent state and is
/// no longer usable for query analysis.
class RelationGraph {
public:
    /// The default constructor creates an empty relation graph.
    RelationGraph() {}

    /// This constructor creates a relation graph from a SELECT
    /// statement.
    RelationGraph(query::QueryContext const& ctx,
                  query::SelectStmt& stmt,
                  TableInfoPool& pool);

    /// `empty` returns `true` if this graph has no vertices.
    bool empty() const { return _vertices.empty(); }

    void swap(RelationGraph& g) {
        _vertices.swap(g._vertices);
        _map.swap(g._map);
    }

private:
    std::list<Vertex> _vertices;
    ColumnVertexMap _map;

    // Not implemented
    RelationGraph(RelationGraph const&);
    RelationGraph& operator=(RelationGraph const&);

    // Constructors and related helpers
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
