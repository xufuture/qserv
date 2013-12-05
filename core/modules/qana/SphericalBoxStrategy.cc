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
  * @file SphericalBoxStrategy.cc
  *
  * @brief SphericalBoxStrategy implementation
  *
  * SphericalBoxStrategy aims to collect the behavior for handling
  * partitioning-related decisions specific to the spherical-box
  * partitioning scheme that are not part of other partitioning
  * schemes like hash-partitioning or 1D range-partitioning.
  *
  * @author Daniel L. Wang, SLAC
  */
#include "qana/SphericalBoxStrategy.h"
#include <sstream>
#include <deque>
#include <boost/lexical_cast.hpp>

#include "css/Facade.h"
#include "query/FromList.h"
#include "qana/QueryMapping.h"
#include "query/QueryContext.h"
#include "parser/ParseException.h"

#include "log/Logger.h"

#define CHUNKTAG "%CC%"
#define SUBCHUNKTAG "%SS%"
#define FULLOVERLAPSUFFIX "FullOverlap"

#define DEBUG 1

namespace { // File-scope helpers
struct Tuple {
    Tuple(std::string const& db_,
          std::string const& prePatchTable_,
          std::string const& alias_)
        : db(db_),
          prePatchTable(prePatchTable_),
          alias(alias_),
          chunkLevel(-1) {
    }
    std::string db;
    std::list<std::string> tables;
    std::string prePatchTable;
    std::string alias;
    int allowed;
    int chunkLevel;
};

typedef std::deque<Tuple> Tuples;

std::ostream& operator<<(std::ostream& os, Tuple const& t) {

    os << t.db << ".";
    os << "(" << t.prePatchTable << ")";
    std::copy(t.tables.begin(), t.tables.end(),
              std::ostream_iterator<std::string>(os, ","));
    os << "_c" << t.chunkLevel << "_";
    if(!t.allowed) { os << "ILLEGAL"; }
    return os;
}

inline void addChunkMap(lsst::qserv::qana::QueryMapping& m) {
    m.insertEntry(CHUNKTAG, lsst::qserv::qana::QueryMapping::CHUNK);
}
inline void addSubChunkMap(lsst::qserv::qana::QueryMapping& m) {
    m.insertEntry(SUBCHUNKTAG, lsst::qserv::qana::QueryMapping::SUBCHUNK);
}
/// @return count of chunked tables.
int patchTuples(Tuples& tuples) {
    using lsst::qserv::qana::SphericalBoxStrategy;
    // Are multiple subchunked tables involved? Then do
    // overlap... which requires creating a query sequence.
    // For now, skip the sequence part.
    // TODO: need to refactor a bit to allow creating a sequence.

    // If chunked table count > 1, use highest chunkLevel and turn on
    // subchunking.
    Tuples::iterator i = tuples.begin();
    Tuples::iterator e = tuples.end();
    int chunkedCount = 0;
    for(; i != e; ++i) {
        if(i->chunkLevel > 0) ++chunkedCount;
    }
    for(i = tuples.begin(); i != e; ++i) {
        std::string const& prePatch = i->prePatchTable;
        switch(i->chunkLevel) {
        case 0:
            i->tables.push_back(prePatch);
            break;
        case 1:
            i->tables.push_back(SphericalBoxStrategy::makeChunkTableTemplate(prePatch));
            break;
        case 2:
            if(chunkedCount > 1) {
                i->db = SphericalBoxStrategy::makeSubChunkDbTemplate(i->db);
                i->tables.push_back(SphericalBoxStrategy::makeSubChunkTableTemplate(prePatch));
                i->tables.push_back(SphericalBoxStrategy::makeOverlapTableTemplate(prePatch));
            } else {
                i->tables.push_back(SphericalBoxStrategy::makeChunkTableTemplate(prePatch));
            }
            break;
        default:
            throw std::logic_error("Unexpected chunkLevel=" +
                                   boost::lexical_cast<std::string>(i->chunkLevel));
            break;
        }
    }
    return chunkedCount;
}

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qana {

class InvalidTableException : public parser::ParseException {
public:
    InvalidTableException(char const* db, char const* table)
        : parser::ParseException(std::string("Invalid table: ") + db + "." + table)
    {}
    InvalidTableException(std::string const& db, std::string const& table)
        : parser::ParseException(std::string("Invalid table: ") + db + "." + table)
    {}
};
class InvalidDbException : public ParseException {
public:
    InvalidDbException(char const* db)
        : ParseException(std::string("Invalid db: ") + db)
    {}
    InvalidDbException(std::string const& db)
        : ParseException(std::string("Invalid db: ") + db)
    {}
};

class lookupTuple {
public:
    lookupTuple(css::Facade& cssFacade_)
        : cssFacade(cssFacade_)
        {}

    void operator()(Tuple& t) {
        t.allowed = cssFacade.containsDb(t.db);
        if(t.allowed) {
            t.chunkLevel = cssFacade.getChunkLevel(t.db, t.prePatchTable);
            if(t.chunkLevel == -1) {
                t.allowed = false; // No chunk level found: missing/illegal.
                throw InvalidTableException(t.db, t.prePatchTable);
            }
        } else {
            throw InvalidDbException(t.db);
        }
    }
    css::Facade& cssFacade;
};

class SphericalBoxStrategy::Impl {
public:
    friend class SphericalBoxStrategy;
    Impl(query::QueryContext& context_) : context(context_) {}
    template <class C>
    inline void getSubChunkTables(C& tables) {
        for(Tuples::const_iterator i=tuples.begin();
            i != tuples.end(); ++i) {
            if(i->chunkLevel == 2) {
                tables.push_back(i->prePatchTable);
            }
        }
    }
    inline void updateMapping(QueryMapping& m) {
        // Look for subChunked tables
        for(Tuples::const_iterator i=tuples.begin();
            i != tuples.end(); ++i) {
            if(i->chunkLevel == 2) {
                std::string const& table = i->prePatchTable;
                if(table.empty()) {
                    throw std::logic_error("Unknown prePatchTable in QueryMapping");
                }
                // Add them to the list of subchunk table dependencies
                m.insertSubChunkTable(table);
            }
        }
    }
    query::QueryContext& context;
    query::FromList const* fromListPtr;
    Tuples tuples;
    int chunkLevel;
};


//template <typename G, typename A>
class addTable : public query::TableRefN::Func {
public:
    addTable(Tuples& tuples) : _tuples(tuples) {
    }
    virtual void operator()(query::TableRefN& t) {
        std::string table = t.getTable();
        if(table.empty()) return; // Don't add the compound-part of
                                  // compound ref.
        _tuples.push_back(Tuple(t.getDb(), t.getTable(), t.getAlias()));
    }
private:
    Tuples& _tuples;
};

class patchTable : public query::TableRefN::Func {
public:
    typedef Tuples::const_iterator TupleCiter;
    patchTable(Tuples& tuples)
        : _tuples(tuples),
          _i(tuples.begin()),
          _end(tuples.end()) {
    }
    virtual void operator()(query::TableRefN& t) {
        std::string table = t.getTable();
        if(table.empty()) return; // Ignore the compound-part of
                                  // compound ref.
        if(_i == _end) {
            throw std::invalid_argument("TableRefN missing table.");
        }
        // LOGGER_INF << "Patching tablerefn:" << t << std::endl;
        t.setDb(_i->db);
        // Always use the first table. A different function will be
        // used when multiple tables are involved.
        if(_i->tables.empty()) {
            throw std::logic_error("Missing patched table");
        } else {
            t.setTable(_i->tables.front());
        }
        ++_i;
    }
private:
    Tuples& _tuples;
    TupleCiter _i;
    TupleCiter _end;

    // G _generate; // Functor that creates a new alias name
    // A _addMap; // Functor that adds a new alias mapping for matchin
    //            // later clauses.
};
class composeOverlap {
public:
    composeOverlap()
        : listCore(new query::TableRefnList()),
          listOverlap(new query::TableRefnList()),
          _firstSubChunkTable(true) {
    }
    virtual void operator()(Tuple const& t) {
        // Idea: Make a TableRefN from each and add it to each list
        typedef std::list<std::string>::const_iterator Iter;
        Iter i=t.tables.begin();
        Iter e=t.tables.end();
        boost::shared_ptr<query::SimpleTableN> rn1;
        boost::shared_ptr<query::SimpleTableN> rn2;
        rn1.reset(new query::SimpleTableN(t.db, *i, t.alias));
        ++i;
        if(_firstSubChunkTable || (i==e))  {
            rn2.reset(new query::SimpleTableN(*rn1));
        } else {
            rn2.reset(new query::SimpleTableN(t.db, *i, t.alias));
            ++i;
            if(i != e) {
                throw std::logic_error("Unexpected third table entry");
            }
        }
        if(t.chunkLevel == 2) {
            _firstSubChunkTable = false;
        }
        listCore->push_back(rn1);
        listOverlap->push_back(rn2);
    }
    boost::shared_ptr<query::TableRefnList> listCore;
    boost::shared_ptr<query::TableRefnList> listOverlap;
private:
    bool _firstSubChunkTable;
};

////////////////////////////////////////////////////////////////////////
// SphericalBoxStrategy public
////////////////////////////////////////////////////////////////////////
SphericalBoxStrategy::SphericalBoxStrategy(query::FromList const& f,
                                           query::QueryContext& context)
    : _impl(new Impl(context)) {
    _import(const_cast<query::FromList&>(f)); // FIXME: should make a copy.
}

boost::shared_ptr<QueryMapping> 
SphericalBoxStrategy::getMapping() {
    assert(_impl.get());
    boost::shared_ptr<QueryMapping> qm(new QueryMapping());

    LOGGER_DBG << "SphericalBoxStrategy::getMapping() : _impl->chunkLevel : "
               << _impl->chunkLevel << std::endl;
    switch(_impl->chunkLevel) {
    case 0:
        break;
    case 1:
        LOGGER_DBG << "SphericalBoxStrategy::getMapping() : calling  addChunkMap()"
                   << std::endl;
        addChunkMap(*qm);
        break;
    case 2:
        LOGGER_DBG << "SphericalBoxStrategy::getMapping() : calling  addSubChunkMap()"
                   << std::endl;
        addChunkMap(*qm);
        addSubChunkMap(*qm);
        _impl->updateMapping(*qm);
        break;
    default:
        break;
    }
    return qm;
}

/// Patch the FromList to add partitioning substitution strings.
/// FromList should be the same as was used at construction
void 
SphericalBoxStrategy::patchFromList(query::FromList& f) {
    if(&f != _impl->fromListPtr) {
        throw std::logic_error("Attempted to patch a different FromList");
    }

    query::TableRefnList& tList = f.getTableRefnList();

    patchTable pt(_impl->tuples);
    std::for_each(tList.begin(), tList.end(),
                  query::TableRefN::Fwrapper<patchTable>(pt));

    // Now, for each tableref, replace table name with substitutable
    // name and an appropriate mapping
    // "FROM Source" -> "FROM Source_%CC%" ",
    // Mapping: (%CC% -> CHUNK), (%SS% -> SUBCHUNK)
    // FullOverlap/SelfOverlap is specified directly at this point,
    // instead of deferring the mapping later, as in the earlier
    // parser/generation system.


    // Update table refs appropriately
    //patchTableRefs pt(tuples);
    //std::for_each(tList.begin(), tList.end(), pt);
    // FIXME
}
bool
SphericalBoxStrategy::needsMultiple() {
    return _impl->chunkLevel > 1;
}

/// Returns a list of two FromLists. Should only be called when
/// _impl->chunkLevel > 1 (i.e., needsMultiple() returns true).  Right
/// now, we only return two, because the only case so far is for
/// near-neighbor self-joins. In this case, we want:
/// SELECT ... FROM Table_x_y, Table_x_y WHERE...
/// and
/// SELECT ... FROM Table_x_y, TableFullOverlap_x_y WHERE...
std::list<boost::shared_ptr<query::FromList> >
SphericalBoxStrategy::computeNewFromLists() {
    composeOverlap co;
    std::for_each(_impl->tuples.begin(), _impl->tuples.end(), co);
    std::list<boost::shared_ptr<query::FromList> > froms;
    boost::shared_ptr<query::FromList> core(new query::FromList(co.listCore));
    boost::shared_ptr<query::FromList> overlap(new query::FromList(co.listOverlap));
    froms.push_back(core);
    froms.push_back(overlap);
    return froms;
}


////////////////////////////////////////////////////////////////////////
// SphericalBoxStrategy public static
////////////////////////////////////////////////////////////////////////
std::string
SphericalBoxStrategy::makeSubChunkDbTemplate(std::string const& db) {
    std::stringstream ss;
    ss << "Subchunks_" << db << "_" CHUNKTAG;
    return ss.str();
}

std::string
SphericalBoxStrategy::makeOverlapTableTemplate(std::string const& table) {
    std::stringstream ss;
    ss << table << FULLOVERLAPSUFFIX "_" CHUNKTAG "_" SUBCHUNKTAG;
    return ss.str();
}

std::string
SphericalBoxStrategy::makeChunkTableTemplate(std::string const& table) {
    std::stringstream ss;
    ss << table << "_" CHUNKTAG;
    return ss.str();
}

std::string
SphericalBoxStrategy::makeSubChunkTableTemplate(std::string const& table) {
    std::stringstream ss;
    ss << table << "_" CHUNKTAG "_" SUBCHUNKTAG;
    return ss.str();
}

////////////////////////////////////////////////////////////////////////
// SphericalBoxStrategy private
////////////////////////////////////////////////////////////////////////
void 
SphericalBoxStrategy::_import(query::FromList const& f) {
    // Save the FromList ref for a later sanity check.
    _impl->fromListPtr = &f;
    // Idea:
    // construct mapping of TableName to a mappable table name
    // Put essential info into QueryMapping so that a query can be
    // substituted properly using a chunk spec without knowing the
    // strategy.

    // Iterate over FromList elements
    query::TableRefnList const& tList = f.getTableRefnList();

    // What we need to know:
    // Are there partitioned tables? If yes, then make chunked queries
    // (and include mappings). For each tableref that is chunked,
    //
    addTable a(_impl->tuples);;
    std::for_each(tList.begin(), tList.end(),
                  query::TableRefN::Fwrapper<addTable>(a));

    if(!_impl->context.cssFacade) {
        throw std::logic_error("Missing context.cssFacade");
    }
    lookupTuple lookup(*_impl->context.cssFacade);
    std::for_each(_impl->tuples.begin(), _impl->tuples.end(), lookup);
    LOGGER_DBG << "Imported:::::";

    std::copy(_impl->tuples.begin(), _impl->tuples.end(),
              std::ostream_iterator<Tuple>(LOG_STRM(Debug), ","));
    LOGGER_DBG << std::endl;

    // Patch tuples in preparation for patching the FromList
    int chunkedTablesNumber = patchTuples(_impl->tuples);
    if(chunkedTablesNumber > 1) { _impl->chunkLevel = 2; }
    else if(chunkedTablesNumber == 1) { _impl->chunkLevel = 1; }
    else { _impl->chunkLevel = 0; }

    LOGGER_DBG << "SphericalBoxStrategy::_import() : _impl->chunkLevel : "
               << _impl->chunkLevel << std::endl;

    // Patch context with mapping.
    if(_impl->context.queryMapping.get()) {
        _impl->context.queryMapping->update(*getMapping());
    } else {
        _impl->context.queryMapping = getMapping();
    }
}

}}} // namespace lsst::qserv::qana