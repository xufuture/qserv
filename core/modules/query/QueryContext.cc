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
/**
  * @file
  *
  * @brief QueryContext implementation.
  *
  * @author Daniel L. Wang, SLAC
  */

#include "query/QueryContext.h"

// Local headers
#include "query/ColumnRef.h"
#include "query/QsRestrictor.h"

using lsst::qserv::query::DbTablePair;
using lsst::qserv::query::DbTableVector;

namespace lsst {
namespace qserv {
namespace query {

/// Resolve a column ref to a concrete (db,table)
/// @return the concrete (db,table), based on current context.
query::DbTablePair
QueryContext::resolve(boost::shared_ptr<ColumnRef> cr) {
    if(!cr) { return query::DbTablePair(); }

    // If alias, retrieve real reference.
    if(cr->db.empty() && !cr->table.empty()) {
        query::DbTablePair concrete = _tableAliases.get(cr->table);
        if(!concrete.empty()) {
            if(concrete.db.empty()) {
                concrete.db = defaultDb();
            }
            return concrete;
        }
    }
    // Set default db and table.
    DbTablePair p;
    if(cr->table.empty()) { // No db or table: choose first resolver pair
        p = _resolverTables[0];
        // TODO: We can be fancy and check the column name against the
        // schema for the entries on the resolverTables, and choose
        // the matching entry.
    } else if(cr->db.empty()) { // Table, but not alias.
        // Match against resolver stack
        for(DbTableVector::const_iterator i=_resolverTables.begin(),
                e=_resolverTables.end();
            i != e; ++i) {
            if(i->table == cr->table) {
                p = *i;
                break;
            }
        }
        return DbTablePair(); // No resolution.
    } else { // both table and db exist, so return them
        return DbTablePair(cr->db, cr->table);
    }
    if(p.db.empty()) {
        // Fill partially-resolved empty db with user db context
        p.db = defaultDb();
    }
    return p;
}

boost::shared_ptr<ConstraintVector> 
QueryContextRestrictors::getConstraints() const {
    boost::shared_ptr<ConstraintVector> cv;
    boost::shared_ptr<QsRestrictor::List const> p = _restrictors;

    if(nRestrictors() > 0) {
        cv.reset(new ConstraintVector(nRestrictors()));
        int i=0;
        QsRestrictor::List::const_iterator li;
        for(li = _restrictors->begin(); li != _restrictors->end(); ++li) {
            Constraint c;
            QsRestrictor const& r = **li;
            c.name = r._name;
            util::StringList::const_iterator si;
            for(si = r._params.begin(); si != r._params.end(); ++si) {
                c.params.push_back(*si);
            }
            (*cv)[i] = c;
            ++i;
        }
        //printConstraints(*cv);
        return cv;
    } else {
        //LOGGER_INF << "No constraints." << std::endl;
    }
    // No constraint vector
    return cv;
}

void
QueryContextRestrictors::mergeInRestrictors(
                             boost::shared_ptr<QsRestrictor::List> keyPreds) {
    if(!_restrictors) {
        _restrictors = keyPreds;
    } else {
        _restrictors->insert(_restrictors->end(),
                             keyPreds->begin(), keyPreds->end());
    }
}

}}} // namespace lsst::qserv::query
