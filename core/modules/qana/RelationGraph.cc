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
/// \brief Implementation of query validation/rewriting.

#include "RelationGraph.h"

// System headers
#include <algorithm>
#include <limits>
#include <stdexcept>

// Local headers
#include "QueryNotEvaluableError.h"

#include "parser/SqlSQL2Parser.hpp" // (generated) SqlSQL2TokenTypes
#include "query/FromList.h"
#include "query/FuncExpr.h"
#include "query/Predicate.h"
#include "query/QueryTemplate.h"
#include "query/SelectList.h"
#include "query/ValueExpr.h"
#include "query/ValueFactor.h"
#include "query/WhereClause.h"


namespace lsst {
namespace qserv {
namespace qana {

using std::list;
using std::logic_error;
using std::min;
using std::numeric_limits;
using std::string;
using std::vector;

using boost::dynamic_pointer_cast;

using query::AndTerm;
using query::BoolFactor;
using query::BoolTerm;
using query::ColumnRef;
using query::CompPredicate;
using query::FuncExpr;
using query::JoinRef;
using query::JoinRefList;
using query::JoinSpec;
using query::QueryContext;
using query::QueryTemplate;
using query::SelectStmt;
using query::TableRef;
using query::TableRefList;
using query::ValueExprList;
using query::ValueExprPtr;
using query::ValueFactor;
using query::ValueFactorPtr;


// ----------------------------------------------------------------
// Vertex implementation

void Vertex::insert(Edge const& e) {
    typedef vector<Edge>::iterator Iter;
    Iter i = std::lower_bound(edges.begin(), edges.end(), e);
    if (i == edges.end() || *i != e) {
        edges.insert(i, e);
    } else {
        // e is a necessarily a duplicate of *i, unless both are
        // director → director edges. In that case, if both edges are spatial,
        // retain the smaller angular constraint. Otherwise, retain the
        // non-spatial edge.
        bool si = i->isSpatial();
        bool se = e.isSpatial();
        if (si || se) {
            if (si && se) {
                i->angSep = min(e.angSep, i->angSep);
            } else {
                // director self-join
                i->angSep = numeric_limits<double>::quiet_NaN();
            }
        }
    }
}


// ----------------------------------------------------------------
// RelationGraph implementation

namespace {

bool isOuterJoin(JoinRef::Type jt) {
    return jt == JoinRef::LEFT || jt == JoinRef::RIGHT || jt == JoinRef::FULL;
}

JoinRef::Type commute(JoinRef::Type jt) {
    if (jt == JoinRef::LEFT) {
        return JoinRef::RIGHT;
    } else if (jt == JoinRef::RIGHT) {
        return JoinRef::LEFT;
    }
    return jt;
}

/// `getColumnRef` returns the ColumnRef in `ve` if there is one.
ColumnRef::Ptr getColumnRef(ValueExprPtr const& ve) {
    ColumnRef::Ptr cr;
    if (!ve || ve->getFactorOps().size() != 1) {
        return cr;
    }
    ValueFactorPtr vf = ve->getFactorOps().front().factor;
    if (!vf) {
        return cr;
    }
    return vf->getColumnRef();
}

/// `verifyColumnRef` checks that a column reference has a column name and an
/// empty database name (because at this stage, fully qualified names should
/// have been rewritten to use a table alias).
void verifyColumnRef(ColumnRef const& c) {
    if (c.column.empty()) {
        throw logic_error(
            "Parser/query analysis bug: "
            "ColumnRef with an empty column name.");
    } else if (!c.db.empty()) {
        if (c.table.empty()) {
            throw logic_error(
                "Parser/query analysis bug: ColumnRef has an empty "
                "table/alias name but a non-empty database name.");
        }
        throw logic_error(
            "Query analysis bug: the db.table portion of a fully "
            "qualified column name was not replaced with an alias.");
    }
}

/// `verifyJoin` throws an exception if the given join parameters are invalid
/// or unsupported.
void verifyJoin(JoinRef::Type joinType,
                bool natural,
                JoinSpec::Ptr const& joinSpec)
{
    switch (joinType) {
        case JoinRef::UNION:
            // "table1 UNION JOIN table2" is probably the same thing as
            // "table1 FULL OUTER JOIN table2 ON FALSE". It is deprecated in
            // SQL99 and removed from SQL2003. Bail out because MySQL supports
            // neither union nor full outer joins.
            throw QueryNotEvaluableError(
                "UNION JOIN queries are not currently supported.");
        case JoinRef::FULL:
            // MySQL does not support full outer joins. Though it is possible
            // to rewrite a full outer join as a UNION of a LEFT and RIGHT join
            // (in the absence of aggregation), this is complicated and likely
            // slow, so bail out.
            throw QueryNotEvaluableError(
                "FULL OUTER JOIN queries are not currently supported.");
        case JoinRef::CROSS:
            if (natural || joinSpec) {
                throw logic_error(
                    "Parser/query analysis bug: a CROSS JOIN cannot be "
                    "NATURAL or have an ON or USING clause.");
            }
            break;
        case JoinRef::INNER: // fallthrough
        case JoinRef::LEFT:  // fallthrough
        case JoinRef::RIGHT:
            if (natural && joinSpec) {
                throw logic_error(
                    "Parser/query analysis bug: a JOIN cannot be NATURAL "
                    "and have an ON or USING clause.");
            }
            break;
        default:
            throw logic_error(
                "Parser/query analysis bug: unrecognized join type.");
    }
}

/// `makeEqEdge` checks whether an equality predicate involving column `ca`
/// from the table reference in `a` and `cb` from `b` is admissible, and
/// creates corresponding `Edge` objects if so. The number of edges created,
/// 0 or 1, is returned.
size_t makeEqEdge(string const& ca,
                  string const& cb,
                  JoinRef::Type jt,
                  Vertex* a,
                  Vertex* b)
{
    if (a == b) {
        return 0;
    }
    bool admissible = false;
    // Check whether the equality predicate is admissible.
    switch (Edge::classify(*a, *b)) {
        case Edge::DIRECTOR_DIRECTOR:
        {
            DirTableInfo const* da = dynamic_cast<DirTableInfo const*>(a->info);
            DirTableInfo const* db = dynamic_cast<DirTableInfo const*>(b->info);
            if (da == db) {
                // The directors are the same (self-join).
                admissible = true;
            }
            break;
        }
        case Edge::DIRECTOR_CHILD:
        {
            DirTableInfo const* d = dynamic_cast<DirTableInfo const*>(a->info);
            ChildTableInfo const* c = dynamic_cast<ChildTableInfo const*>(b->info);
            if (d == c->director) {
                // Child's director table is the director being joined with
                admissible = true;
            }
            break;
        }
        case Edge::DIRECTOR_MATCH:
        {
            DirTableInfo const* d = dynamic_cast<DirTableInfo const*>(a->info);
            MatchTableInfo const* m = dynamic_cast<MatchTableInfo const*>(b->info);
            if ((m->director.first == d && m->fk.first == cb) ||
                (m->director.second == d && m->fk.second == cb)) {
                // Director is the same as the corresponding match table director
                admissible = !isOuterJoin(jt);
            }
            break;
        }
        case Edge::CHILD_CHILD:
        {
            ChildTableInfo const* c1 = dynamic_cast<ChildTableInfo const*>(a->info);
            ChildTableInfo const* c2 = dynamic_cast<ChildTableInfo const*>(b->info);
            if (c1->director == c2->director) {
                // Both child tables have the same director
                admissible = true;
            }
            break;
        }
        case Edge::CHILD_MATCH:
        {
            ChildTableInfo const* c = dynamic_cast<ChildTableInfo const*>(a->info);
            DirTableInfo const* d = c->director;
            MatchTableInfo const* m = dynamic_cast<MatchTableInfo const*>(b->info);
            if ((m->director.first == d && m->fk.first == cb) ||
                (m->director.second == d && m->fk.second == cb)) {
                // Child's director is the same as the corresponding match table director
                admissible = !isOuterJoin(jt);
            }
            break;
        }
        case Edge::MATCH_MATCH: // fallthrough
        default:
            break;
        case Edge::CHILD_DIRECTOR: // fallthrough
        case Edge::MATCH_DIRECTOR: // fallthrough
        case Edge::MATCH_CHILD:
            // Swap vertices and recurse to avoid code stutter.
            return makeEqEdge(cb, ca, commute(jt), b, a);
    }
    if (admissible) {
        // Add a pair of edges, a → b and b → a.
        a->insert(Edge(b, numeric_limits<double>::quiet_NaN()));
        b->insert(Edge(a, numeric_limits<double>::quiet_NaN()));
    }
    return admissible;
}

/// `getNumericConst` returns the numeric constant embedded in the given
/// value expression if there is one, and NaN otherwise.
double getNumericConst(ValueExprPtr const& ve) {
    if (!ve || ve->getFactorOps().size() != 1) {
        return numeric_limits<double>::quiet_NaN();
    }
    ValueFactorPtr vf = ve->getFactorOps().front().factor;
    if (!vf || vf->getType() != ValueFactor::CONST) {
        return numeric_limits<double>::quiet_NaN();
    }
    char *e = NULL;
    double a = std::strtod(vf->getTableStar().c_str(), &e);
    if (e == vf->getTableStar().c_str()) {
        // conversion error - non-numeric constant
        return numeric_limits<double>::quiet_NaN();
    }
    return a;
}

/// `getAngSepFunc` returns a pointer to the IR node for the `scisql_angSep`
/// call embedded in the given value expression if there is one, and null
/// otherwise.
FuncExpr::Ptr getAngSepFunc(ValueExprPtr const& ve) {
    FuncExpr::Ptr fe;
    if (!ve || ve->getFactorOps().size() != 1) {
        return fe;
    }
    ValueFactorPtr vf = ve->getFactorOps().front().factor;
    if (!vf || vf->getType() != ValueFactor::FUNCTION) {
        return fe;
    }
    fe = vf->getFuncExpr();
    if (!fe || fe->name != "scisql_angSep" || fe->params.size() != 4) {
        return FuncExpr::Ptr();
    }
    return fe;
}

} // unnamed namespace


/// `_makeOnEqEdges` creates a graph edge for each admissible top-level
/// equality predicate extracted from the ON clause of the join between table
/// references in this graph and `g`. The number of admissible predicates is
/// returned.
size_t RelationGraph::_makeOnEqEdges(BoolTerm::Ptr on,
                                     JoinRef::Type jt,
                                     RelationGraph& g)
{
    size_t numEdges = 0;
    on = findAndTerm(on);
    AndTerm::Ptr a = dynamic_pointer_cast<AndTerm>(on);
    if (a) {
        // Recurse to the children.
        typedef BoolTerm::PtrList::const_iterator BtIter;
        for (BtIter i = a->_terms.begin(), e = a->_terms.end(); i != e; ++i) {
            numEdges += _makeOnEqEdges(*i, jt, g);
        }
        return numEdges;
    }
    // Look for a BoolFactor containing a single CompPredicate.
    BoolFactor::Ptr bf = dynamic_pointer_cast<BoolFactor>(on);
    if (!bf || bf->_terms.size() != 1) {
        return 0;
    }
    CompPredicate::Ptr cp =
        dynamic_pointer_cast<CompPredicate>(bf->_terms.front());
    if (!cp || cp->op != SqlSQL2TokenTypes::EQUALS_OP) {
        return 0;
    }
    // Extract column references (if they exist)
    ColumnRef::Ptr l = getColumnRef(cp->left);
    ColumnRef::Ptr r = getColumnRef(cp->right);
    if (!l || !r) {
        return 0;
    }
    verifyColumnRef(*l);
    verifyColumnRef(*r);
    // Lookup column references in graphs being joined together
    vector<Vertex*> const& al = _map.find(*l);
    vector<Vertex*> const& bl = g._map.find(*l);
    vector<Vertex*> const& ar = _map.find(*r);
    vector<Vertex*> const& br = g._map.find(*r);
    if ((!al.empty() && !bl.empty()) || (!ar.empty() && !br.empty())) {
        // At least one column reference was found in both graphs
        QueryTemplate qt;
        (al.empty() ? r : l)->renderTo(qt);
        throw QueryNotEvaluableError("Column reference " + qt.generate() +
                                     " is ambiguous");
    }
    if ((al.empty() && bl.empty()) || (ar.empty() && br.empty())) {
        // At least one column reference wasn't found
        return 0;
    }
    if ((!al.empty() && !ar.empty()) || (!bl.empty() && !br.empty())) {
        // Both column references were found in the same graph. The predicate
        // cannot be used for partition inference if it comes from the ON
        // clause of an outer join. To see why, consider the following query:
        //
        // SELECT * FROM (A JOIN B) LEFT JOIN C ON A.id = B.id AND B.id = C.id;
        //
        // This query can return rows with A.id != B.id, in which case columns
        // from C will be filled in with NULLs. On the other hand, if the query
        // is:
        //
        // SELECT * FROM A LEFT JOIN B ON A.id = B.id;
        //
        // then the predicate is usable for partition inference, since all
        // results will satisfy A.id = B.id OR B.id IS NULL, and checking
        // whether or not a row r from A matches any rows in B only requires
        // looking at rows from B that have the same partition as r.
        if (isOuterJoin(jt)) {
            return 0;
        }
    }
    vector<Vertex*> const& v1 = al.empty() ? bl : al;
    vector<Vertex*> const& v2 = ar.empty() ? br : ar;
    typedef vector<Vertex*>::const_iterator VertIter;
    for (VertIter i1 = v1.begin(), e1 = v1.end(); i1 != e1; ++i1) {
        for (VertIter i2 = v2.begin(), e2 = v2.end(); i2 != e2; ++i2) {
            numEdges += makeEqEdge(l->column, r->column, jt, *i1, *i2);
        }
    }
    return numEdges;
}

/// `_makeNaturalEqEdges` constructs an edge for each (implicit) admissible
/// equality predicate in the natural join between table references from this
/// graph and `g`. The number of admissible predicates is returned.
size_t RelationGraph::_makeNaturalEqEdges(JoinRef::Type jt,
                                          RelationGraph& g)
{
    typedef vector<string>::const_iterator ColIter;
    typedef vector<Vertex*>::const_iterator VertIter;

    vector<string> const cols = _map.computeCommonCols(g._map);
    string const _;
    size_t numEdges = 0;
    for (ColIter c = cols.begin(), e = cols.end(); c != e; ++c) {
        ColumnRef const cr(_, _, *c);
        vector<Vertex*> const& v1 = _map.find(cr);
        vector<Vertex*> const& v2 = g._map.find(cr);
        for (VertIter i1 = v1.begin(), e1 = v1.end(); i1 != e1; ++i1) {
            for (VertIter i2 = v2.begin(), e2 = v2.end(); i2 != e2; ++i2) {
                numEdges += makeEqEdge(*c, *c, jt, *i1, *i2);
            }
        }
    }
    return numEdges;
}

/// `_makeUsingEqEdges` constructs an edge for each admissible equality
/// predicate implied by the USING clause of a join between table references
/// from this graph and `g`. The number of admissible predicates is returned.
size_t RelationGraph::_makeUsingEqEdges(ColumnRef const& c,
                                        JoinRef::Type jt,
                                        RelationGraph& g)
{
    typedef vector<Vertex*>::const_iterator VertIter;

    if (!c.db.empty() || !c.table.empty()) {
        throw QueryNotEvaluableError(
            "USING clause contains qualified column name");
    }
    vector<Vertex*> const& v1 = _map.find(c);
    vector<Vertex*> const& v2 = g._map.find(c);
    size_t numEdges = 0;
    for (VertIter i1 = v1.begin(), e1 = v1.end(); i1 != e1; ++i1) {
        for (VertIter i2 = v2.begin(), e2 = v2.end(); i2 != e2; ++i2) {
            numEdges += makeEqEdge(c.column, c.column, jt, *i1, *i2);
        }
    }
    return numEdges;
}

/// `_makeWhereEqEdges` creates a graph edge for each admissible top-level
/// equality predicate extracted from the WHERE clause of a query. The number
/// of admissible predicates is returned.
size_t RelationGraph::_makeWhereEqEdges(BoolTerm::Ptr where)
{
    size_t numEdges = 0;
    where = findAndTerm(where);
    AndTerm::Ptr a = dynamic_pointer_cast<AndTerm>(where);
    if (a) {
        // Recurse to the children.
        typedef BoolTerm::PtrList::const_iterator BtIter;
        for (BtIter i = a->_terms.begin(), e = a->_terms.end(); i != e; ++i) {
            numEdges += _makeWhereEqEdges(*i);
        }
        return numEdges;
    }
    // Look for a BoolFactor containing a single CompPredicate.
    BoolFactor::Ptr bf = dynamic_pointer_cast<BoolFactor>(where);
    if (!bf || bf->_terms.size() != 1) {
        return 0;
    }
    CompPredicate::Ptr cp =
        dynamic_pointer_cast<CompPredicate>(bf->_terms.front());
    if (!cp || cp->op != SqlSQL2TokenTypes::EQUALS_OP) {
        return 0;
    }
    // Extract column references (if they exist)
    ColumnRef::Ptr l = getColumnRef(cp->left);
    ColumnRef::Ptr r = getColumnRef(cp->right);
    if (!l || !r) {
        return 0;
    }
    // Verify and lookup column references
    verifyColumnRef(*l);
    verifyColumnRef(*r);
    vector<Vertex*> const& v1 = _map.find(*l);
    vector<Vertex*> const& v2 = _map.find(*r);
    // Create admissible edges
    typedef vector<Vertex*>::const_iterator VertIter;
    for (VertIter i1 = v1.begin(), e1 = v1.end(); i1 != e1; ++i1) {
        for (VertIter i2 = v2.begin(), e2 = v2.end(); i2 != e2; ++i2) {
            numEdges += makeEqEdge(
                l->column, r->column, JoinRef::INNER, *i1, *i2);
        }
    }
    return numEdges;
}

/// `_makeSpEdges` creates a graph edge for each admissible top-level spatial
/// predicate extracted from the given boolean term. The number of admissible
/// predicates is returned.
size_t RelationGraph::_makeSpEdges(BoolTerm::Ptr term,
                                   double overlap)
{
    size_t numEdges = 0;
    term = findAndTerm(term);
    AndTerm::Ptr a = dynamic_pointer_cast<AndTerm>(term);
    if (a) {
        // Recurse to the children.
        typedef BoolTerm::PtrList::const_iterator BtIter;
        for (BtIter i = a->_terms.begin(), e = a->_terms.end(); i != e; ++i) {
            numEdges += _makeSpEdges(*i, overlap);
        }
        return numEdges;
    }
    // Look for a BoolFactor containing a single CompPredicate.
    BoolFactor::Ptr bf = dynamic_pointer_cast<BoolFactor>(term);
    if (!bf || bf->_terms.size() != 1) {
        return 0;
    }
    CompPredicate::Ptr cp =
        dynamic_pointer_cast<CompPredicate>(bf->_terms.front());
    if (!cp) {
        return 0;
    }
    // Try to extract a scisql_angSep() call and a numeric constant
    // from the comparison predicate.
    FuncExpr::Ptr fe;
    double x = numeric_limits<double>::quiet_NaN();
    switch (cp->op) {
        case SqlSQL2TokenTypes::LESS_THAN_OP: // fallthrough
        case SqlSQL2TokenTypes::LESS_THAN_OR_EQUALS_OP:
            fe = getAngSepFunc(cp->left);
            x = getNumericConst(cp->right);
            break;
        case SqlSQL2TokenTypes::GREATER_THAN_OP: // fallthrough
        case SqlSQL2TokenTypes::GREATER_THAN_OR_EQUALS_OP:
            x = getNumericConst(cp->left);
            fe = getAngSepFunc(cp->right);
            break;
        case SqlSQL2TokenTypes::EQUALS_OP:
            // While this doesn't make much sense numerically (floating
            // point numbers are being tested for equality), it is
            // technically evaluable.
            fe = getAngSepFunc(cp->left);
            if (!fe) {
                x = getNumericConst(cp->left);
                fe = getAngSepFunc(cp->right);
            } else {
                x = getNumericConst(cp->right);
            }
            break;
    }
    if (!fe || (boost::math::isnan)(x) || x > overlap) {
        // The scisql_angSep() call and/or numeric constant is missing,
        // the constant exceeds the partition overlap or the comparison
        // operator is invalid (e.g. "x < scisql_angSep(...)")
        return 0;
    }
    // Extract column references from fe
    ValueExprList::const_iterator j = fe->params.begin();
    ColumnRef::Ptr cr[4];
    Vertex* v[4];
    for (int i = 0; i < 4; ++i) {
        cr[i] = getColumnRef(*j++);
        if (!cr[i]) {
            // Argument i is not a column reference
            return 0;
        }
        vector<Vertex*> const& vv = _map.find(*cr[i]);
        if (vv.empty()) {
            // Column reference not found
            return 0;
        }
        v[i] = vv.front();
    }
    // For the predicate to be admissible, the columns in each coordinate
    // pair must come from the same table reference. Additionally, the two
    // coordinate pairs must come from different table references.
    if (v[0] != v[1] || v[2] != v[3] || v[0] == v[2]) {
        return 0;
    }
    // Check that both column pairs were found in director tables
    DirTableInfo const* d1 = dynamic_cast<DirTableInfo const*>(v[0]->info);
    DirTableInfo const* d2 = dynamic_cast<DirTableInfo const*>(v[2]->info);
    if (!d1 || !d2) {
        return 0;
    }
    // Check that the arguments are the director's spatial columns
    if (cr[0]->column != d1->lon || cr[1]->column != d1->lat ||
        cr[2]->column != d2->lon || cr[3]->column != d2->lat) {
        return 0;
    }
    // Check that both directors have the same partitioning
    if (d1->partitioningId != d2->partitioningId) {
        return 0;
    }
    // Finally, add an edge between v[0] and v[2].
    v[0]->insert(Edge(v[2], x));
    v[2]->insert(Edge(v[0], x));
    return 1;
}

/// `join` splices the relation graph `g` into this one, adding edges
/// for all admissible join predicates extracted from the given join
/// parameters. `g` is emptied as a result.
void RelationGraph::_join(JoinRef::Type joinType,
                          bool natural,
                          JoinSpec::Ptr const& joinSpec,
                          double overlap,
                          RelationGraph& g)
{
    if (this == &g) {
        throw logic_error("A RelationGraph cannot be join()ed with itself.");
    }
    verifyJoin(joinType, natural, joinSpec);
    // Deal with replicated relations
    if (empty()) {
        if (g.empty()) {
            // Arbitrary joins are allowed between replicated relations,
            // and there is no need to store any information about them.
            return;
        }
        // In general, "A LEFT JOIN B" is not evaluable if A is replicated and
        // B is partitioned. While there are specific cases that do work (e.g.
        // "A LEFT JOIN B ON FALSE"), the effort to detect them does noe seem
        // worthwhile.
        if (joinType == JoinRef::LEFT) {
            throw QueryNotEvaluableError(
                "Query contains a LEFT JOIN between replicated and "
                "partitioned tables.");
        }
        swap(g);
        return;
    } else if (g.empty()) {
        // In general, "A RIGHT JOIN B" is not evaluable if A is partitioned
        // and B is replicated.
        if (joinType == JoinRef::RIGHT) {
            throw QueryNotEvaluableError(
                "Query contains a RIGHT JOIN between partitioned and "
                "replicated tables.");
        }
        return;
    }
    size_t numEdges = 0;
    if (natural) {
        numEdges += _makeNaturalEqEdges(joinType, g);
    } else if (joinSpec && joinSpec->getUsing()) {
        ColumnRef const& c = *joinSpec->getUsing();
        numEdges += _makeUsingEqEdges(c, joinType, g);
    } else if (joinSpec && joinSpec->getOn()) {
        numEdges += _makeOnEqEdges(joinSpec->getOn(), joinType, g);
    }
    if (isOuterJoin(joinType) && numEdges == 0) {
        // For outer joins, require the presence of at least one admissible
        // join predicate. Doing this means that determining whether or not
        // a row from the left and/or right relation of an outer join has a
        // match on the right/left only requires looking at data from the
        // same partition. For inner joins, admissible predicates can be
        // provided later (e.g. in the WHERE clause).
        throw QueryNotEvaluableError(
            "Unable to evaluate query by joining only partition-local data");
    }
    // Splice g into this graph.
    _vertices.splice(_vertices.end(), g._vertices);
    _map.splice(g._map, natural);
    // Add spatial edges
    if (!isOuterJoin(joinType) && joinSpec && joinSpec->getOn()) {
        _makeSpEdges(joinSpec->getOn(), overlap);
    }
}

/// This constructor creates a relation graph for a single partitioned
/// table reference.
RelationGraph::RelationGraph(TableRef& tr,
                             TableInfo const* info,
                             double overlap)
{
    typedef vector<ColumnRefConstPtr>::const_iterator Iter;

    if (!info) {
        return;
    } else if (info->kind != TableInfo::MATCH) {
        _vertices.push_back(Vertex(tr, info));
        ColumnVertexMap m(_vertices.front());
        _map.swap(m);
    } else {
        // Decompose match table references into a pair of vertices - one for
        // each foreign key in the match table.
        _vertices.push_back(Vertex(tr, info));
        _vertices.push_back(Vertex(tr, info));
        // Create a spatial edge between the vertex pair. Note that if the
        // match table metadata included the maximum angular separation between
        // matched entities, it could be used instead of the partition overlap
        // below (the latter is an upper bound on the former).
        _vertices.front().insert(Edge(&_vertices.back(), overlap));
        _vertices.back().insert(Edge(&_vertices.front(), overlap));
        // Split column references for the match table reference across vertices.
        vector<ColumnRefConstPtr> refs = info->makeColumnRefs(tr.getAlias());
        std::sort(refs.begin(), refs.end(), ColumnRefLt());
        Iter begin = refs.begin();
        Iter middle = begin;
        Iter end = refs.end();
        for (; middle != end; ++middle) {
            if ((*middle)->column != refs.front()->column) {
                break;
            }
        }
        ColumnVertexMap m1(_vertices.front(), begin, middle);
        ColumnVertexMap m2(_vertices.back(), middle, end);
        m1.splice(m2, false);
        _map.swap(m1);
    }
}

/// This constructor creates a relation graph for a `TableRef`
/// and its constituent joins.
RelationGraph::RelationGraph(QueryContext const& ctx,
                             TableRef::Ptr const& tr,
                             double overlap,
                             TableInfoPool& pool)
{
    if (!tr) {
        throw logic_error("Parser/query analysis bug: NULL TableRef pointer "
                          "passed to RelationGraph constructor.");
    }
    // Create a graph with at most one vertex using the left table
    // in a join sequence.
    RelationGraph g(*tr, pool.get(ctx, tr->getDb(), tr->getTable()), overlap);
    // Process remaining tables in the JOIN sequence. Note that joins are
    // left-associative in the absence of parentheses, i.e. "A JOIN B JOIN C"
    // is equivalent to "(A JOIN B) JOIN C", and that relation graphs are
    // built in join precedence order. This is important for proper column
    // reference resolution - for instance, an unqualified column reference
    // "foo" might be unambiguous in the ON clause of "A JOIN B", but ambiguous
    // in the ON clause for "(A JOIN B) JOIN C".
    JoinRefList const& joins = tr->getJoins();
    typedef JoinRefList::const_iterator Iter;
    for (Iter i = joins.begin(), e = joins.end(); i != e; ++i) {
        JoinRef& j = **i;
        RelationGraph tmp(ctx, j.getRight(), overlap, pool);
        g._join(j.getJoinType(), j.isNatural(), j.getSpec(), overlap, tmp);
    }
    swap(g);
}


namespace {

// A singly-linked list of vertices. Storage for links is embedded directly
// into the Vertex struct, which allows allows relation graph traversal to
// proceed without memory allocation (at a small complexity cost relative to
// using the STL).
struct VertexQueue {
    Vertex* head;
    Vertex* tail;

    VertexQueue() : head(0), tail(0) {}

    Vertex* dequeue() {
        if (head) {
            Vertex* v = head;
            head = head->next;
            if (!head) {
                tail = 0;
            }
            v->next = 0;
            return v;
        }
        return 0;
    }

    void enqueue(Vertex* v) {
        if (v->next || v == tail) {
            // v is already in the queue
            return;
        }
        if (!head) {
            head = tail = v;
        } else {
            tail->next = v;
            tail = v;
        }
    }
};

void traverse(Vertex* v, double const partitionOverlap)
{
    typedef vector<Edge>::const_iterator Iter;

    VertexQueue q;
    if (v) {
        v->overlap = 0;
    }
    while (v) {
        for (Iter e = v->edges.begin(), end = v->edges.end(); e != end; ++e) {
            Vertex* u = e->vertex;
            double const prevRequiredOverlap = u->overlap;
            double availableOverlap = 0.0;
            if (u->info->kind == TableInfo::DIRECTOR ||
                (v->info->kind == TableInfo::MATCH &&
                 u->info->kind == TableInfo::MATCH)) {
                availableOverlap = partitionOverlap;
            }
            double requiredOverlap = v->overlap;
            if (e->isSpatial()) {
                requiredOverlap += e->angSep;
            }
            if (requiredOverlap < availableOverlap &&
                requiredOverlap < prevRequiredOverlap) {
                // update overlap for u and add it into the processing queue
                u->overlap = requiredOverlap;
                q.enqueue(u);
            }
        }
        // remove a vertex from the processing queue and continue
        v = q.dequeue();
    }
}

/// `isEvaluable` returns `true` if no graph vertex requires infinite overlap.
bool isEvaluable(list<Vertex> const& vertices)
{
    typedef list<Vertex>::const_iterator Iter;
    for (Iter v = vertices.begin(), e = vertices.end(); v != e; ++v) {
        if ((boost::math::isinf)(v->overlap)) {
            return false;
        }
    }
    return true;
}

/// `resetVertices` sets the required overlap of all graph vertices to ∞.
void resetVertices(list<Vertex>& vertices)
{
    typedef list<Vertex>::iterator Iter;
    for (Iter v = vertices.begin(), e = vertices.end(); v != e; ++v) {
        v->overlap = numeric_limits<double>::infinity();
    }
}

} // unnamed namespace

/// `_validate` searches for a graph traversal that proves the input query
/// is evaluable.
bool RelationGraph::_validate(double overlap)
{
    typedef list<Vertex>::iterator Iter;
    size_t numStarts = 0;
    for (Iter i = _vertices.begin(), e = _vertices.end(); i != e; ++i) {
        if (i->info->kind != TableInfo::MATCH) {
            ++numStarts;
            resetVertices(_vertices);
            traverse(&(*i), overlap);
            if (isEvaluable(_vertices)) {
                return true;
            }
        }
    }
    // If there were no traversal starting points, then the input query
    // involves a single match table, and can be evaluated. Otherwise, it
    // is not evaluable.
    return numStarts == 0;
}

RelationGraph::RelationGraph(QueryContext const& ctx,
                             SelectStmt& stmt,
                             TableInfoPool& pool)
{
    // Check that at least one thing is being selected.
    if (!stmt.getSelectList().getValueExprList() ||
        stmt.getSelectList().getValueExprList()->empty()) {
        throw QueryNotEvaluableError("Query has no select list");
    }
    // Check that the FROM clause isn't empty.
    TableRefList const& refs = stmt.getFromList().getTableRefList();
    if (refs.empty()) {
        throw QueryNotEvaluableError(
            "Query must include at least one table reference");
    }
    double overlap = ctx.cssFacade->getOverlap(ctx.dominantDb);
    // Build a graph for the first entry in the from list
    RelationGraph g(ctx, refs.front(), overlap, pool);
    // "SELECT ... FROM A, B, C, ..." is equivalent to
    // "SELECT ... FROM ((A CROSS JOIN B) CROSS JOIN C) ..."
    typedef TableRefList::const_iterator Iter;
    for (Iter i = ++refs.begin(), e = refs.end(); i != e; ++i) {
        RelationGraph tmp(ctx, *i, overlap, pool);
        g._join(JoinRef::CROSS, false, JoinSpec::Ptr(), overlap, tmp);
    }
    // Add edges for admissible join predicates extracted from the
    // WHERE clause
    if (stmt.hasWhereClause()) {
        BoolTerm::Ptr where = stmt.getWhereClause().getRootTerm();
        g._makeWhereEqEdges(where);
        g._makeSpEdges(where, overlap);
    }
    if (!_validate(overlap)) {
        throw QueryNotEvaluableError(
            "Query cannot be evaluated using worker-local data");
    }
    swap(g);
}

}}} // namespace lsst::qserv::qana
