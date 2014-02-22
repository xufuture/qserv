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
#include "qana/TableStrategy.h"
#include <deque>
#include <string>
#include <boost/lexical_cast.hpp>
#include <boost/pointer_cast.hpp>
#include "log/Logger.h"
#include "meta/MetadataCache.h"
#include "query/FromList.h"
#include "query/TableRefN.h"
#include "query/JoinSpec.h"
#include "query/QueryContext.h"


#define CHUNKTAG "%CC%"
#define SUBCHUNKTAG "%SS%"
#define FULLOVERLAPSUFFIX "FullOverlap"


#define DEBUG 1

namespace { // File-scope helpers

}

#if 0 ////////////////////////////////////////////////////////////////////////
std::ostream& operator<<(std::ostream& os, Tuple const& t) {

    os << t.db << ".";
    os << "(" << t.prePatchTable << ")";
    std::copy(t.tables.begin(), t.tables.end(),
              std::ostream_iterator<std::string>(os, ","));
    os << "_c" << t.chunkLevel << "_";
    if(!t.allowed) { os << "ILLEGAL"; }
    return os;
}

inline void addChunkMap(lsst::qserv::master::QueryMapping& m) {
#endif ////////////////////////////////////////////////////////////////////////
namespace lsst {
namespace qserv {
namespace master {
class InvalidTableException : public std::logic_error {
public:
    InvalidTableException(char const* db, char const* table)
        : std::logic_error(std::string("Invalid table: ") + db + "." + table)
        {}
    InvalidTableException(std::string const& db, std::string const& table)
        : std::logic_error(std::string("Invalid table: ") + db + "." + table)
        {}
};


struct Tuple {
    Tuple(std::string const& db_,
          std::string const& prePatchTable_,
          std::string const& alias_,
          TableRefN const* node_)
        : db(db_),
          prePatchTable(prePatchTable_),
          alias(alias_),
          chunkLevel(-1),
          node(node_) { node = 0;
    }
    Tuple(TableRefN const* node_) :
        db(""), prePatchTable(""), alias(""),
        node(node_) { node = 0;}
    std::string db;
    std::list<std::string> tables;
    std::string prePatchTable;
    std::string alias;
    int allowed;
    int chunkLevel;
    TableRefN const* node;
};
typedef std::deque<Tuple> Tuples;

class TableNamer {
public:
    static std::string makeSubChunkDbTemplate(std::string const& db) {
        return "Subchunks_" + db + "_" CHUNKTAG;
    }

    static std::string makeOverlapTableTemplate(std::string const& table) {
        return table + FULLOVERLAPSUFFIX "_" CHUNKTAG "_" SUBCHUNKTAG;
    }

    static std::string makeChunkTableTemplate(std::string const& table) {
        return table + "_" CHUNKTAG;
    }

    static std::string makeSubChunkTableTemplate(std::string const& table) {
        return table + "_" CHUNKTAG "_" SUBCHUNKTAG;
    }
    /// @return count of chunked tables.
    static int patchTuples(Tuples& tuples) {
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
                i->tables.push_back(makeChunkTableTemplate(prePatch));
                break;
            case 2:
                if(chunkedCount > 1) {
                    i->db = makeSubChunkDbTemplate(i->db);
                    i->tables.push_back(makeSubChunkTableTemplate(prePatch));
                    i->tables.push_back(makeOverlapTableTemplate(prePatch));
                } else {
                    i->tables.push_back(makeChunkTableTemplate(prePatch));
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
};

inline void updateMappingFromTuples(QueryMapping& m, Tuples const& tuples) {
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

#if 0 ////////////////////////////////////////////////////////////////////////
class patchTable : public TableRefN::Func {
public:
    typedef Tuples::const_iterator TupleCiter;
    patchTable(Tuples& tuples)
        : _tuples(tuples),
          _i(tuples.begin()),
          _end(tuples.end()) {
    }
    virtual void operator()(TableRefN& t) {
        SimpleTableN* p = dynamic_cast<SimpleTableN*>(&t);
        if(p) {
            std::string table = p->getTable();
            if(table.empty()) {
                throw std::logic_error("SimpleTableN: missing table"); }
            if(_i == _end) {
                throw std::invalid_argument("TableRefN missing table.");
            }
            // std::cout << "Patching tablerefn:" << t << std::endl;
            p->setDb(_i->db);
            // Always use the first table. A different function will be
            // used when multiple tables are involved.
            if(_i->tables.empty()) {
                throw std::logic_error("Missing patched table");
            } else {
                p->setTable(_i->tables.front());
            }
            ++_i;
        } else {
            t.apply(*this);
        }
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
        : listCore(new TableRefnList()),
          listOverlap(new TableRefnList()),
          _firstSubChunkTable(true) {
    }
    virtual void operator()(Tuple const& t) {
        // Idea: Make a TableRefN from each and add it to each list
        typedef std::list<std::string>::const_iterator Iter;
        Iter i=t.tables.begin();
        Iter e=t.tables.end();
        boost::shared_ptr<SimpleTableN> rn1;
        boost::shared_ptr<SimpleTableN> rn2;
        rn1.reset(new SimpleTableN(t.db, *i, t.alias));
        ++i;
        if(_firstSubChunkTable || (i==e))  {
            rn2.reset(new SimpleTableN(*rn1));
        } else {
            rn2.reset(new SimpleTableN(t.db, *i, t.alias));
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
    boost::shared_ptr<TableRefnList> listCore;
    boost::shared_ptr<TableRefnList> listOverlap;
private:
    bool _firstSubChunkTable;
};
#endif ////////////////////////////////////////////////////////////////////////

class TableStrategy::Impl {
public:
    Impl(QueryContext& context_) : context(context_) {}
    ~Impl() {}

    QueryContext& context;
    Tuples tuples;
    int chunkLevel;
};

class addTable { //: public TableRefN::Func {
public:
    addTable(Tuples& tuples) : _tuples(tuples) { }
    void operator()(TableRefN::Ptr t) {
        SimpleTableN::Ptr st(boost::dynamic_pointer_cast<SimpleTableN>(t));
        if(st) {
            std::string table = st->getTable();

            if(table.empty()) {
                throw std::logic_error("Missing table in SimpleTableN");
            }
            _tuples.push_back(Tuple(st->getDb(), table,
                                    st->getAlias(), st.get()));
        } else {
            JoinRefN::Ptr jr = boost::dynamic_pointer_cast<JoinRefN>(t);
            if(jr) {
                (*this)(jr->getLeft());
                (*this)(jr->getRight());
            } else {
                // Not Simple and not Join... shouldn't happen.
            }
        }

    }
private:
    Tuples& _tuples;
};
class updateChunkLevel {
public:
    updateChunkLevel(MetadataCache& metadata_)
        : metadata(metadata_)
        {}

    void operator()(Tuple& t) {
        t.allowed = metadata.checkIfContainsDb(t.db);
        if(t.allowed) {
            t.chunkLevel = metadata.getChunkLevel(t.db, t.prePatchTable);
            if(t.chunkLevel == -1) {
                t.allowed = false; // No chunk level found: missing/illegal.
                throw InvalidTableException(t.db, t.prePatchTable);
            }
        }
    }
    MetadataCache& metadata;
};
class inplaceComputeTable {
public:
    // FIXME: How can we consolidate with computeTable?
    inplaceComputeTable(Tuples& tuples) :_tuples(tuples) {
    }
    Tuple& getTuple(SimpleTableN::Ptr t) {
        // FIXME: Switch to map and rethink the system
        typedef Tuples::iterator Iter;
        for(Iter i=_tuples.begin(),e=_tuples.end(); i != e; ++i) {
            if(i->prePatchTable == t->getTable()) { return *i; }
        }
        throw std::logic_error("Not found in tuples");
    }
    virtual void operator()(TableRefN::Ptr t) {
        SimpleTableN::Ptr st(boost::dynamic_pointer_cast<SimpleTableN>(t));
        if(st) {
            Tuple& tuple = getTuple(st);
            st->setDb(tuple.db);
            st->setTable(tuple.tables.front());
        } else {
            JoinRefN::Ptr jr = boost::dynamic_pointer_cast<JoinRefN>(t);
            if(!jr) {
                throw std::logic_error("TableRefN not SimpleTableN or JoinRefN");
            }
            (*this)(jr->getLeft());
            (*this)(jr->getRight());
        }
    }
    Tuples& _tuples;
};
class computeTable : public TableRefN::Func {
public:
    computeTable(Tuples& tuples, int permutation)
        : _tuples(tuples), _permutation(permutation) {
        // Should already know how many permutations. 0 - (n-1)
    }
    virtual TableRefN::Ptr operator()(TableRefN::Ptr t) {
        return visit(t);
        // See if tuple matches table.
        // if t in tuples,
        // else, if simple, return copy
        // if not simple, visit both sides.


        // if match, replace. otherwise, it's compound. For now, just
        // visit both sides of the join.
    }
    inline TableRefN::Ptr visit(TableRefN::Ptr t) {
        int perm = 0;
        TableRefN::Ptr newT = lookup(t, perm);
        if(newT) { return newT; }
        else if(t->isSimple()) {
            // else, if simple, return copy
            SimpleTableN::Ptr st(boost::dynamic_pointer_cast<SimpleTableN>(t));
            std::cout << "passthrough table: " << st->getTable() << std::endl;
            return st->clone();
        }
        JoinRefN::Ptr jr = boost::dynamic_pointer_cast<JoinRefN>(t);
        if(!jr) {
            throw std::logic_error("TableRefN not SimpleTableN or JoinRefN");
        }
        boost::shared_ptr<JoinSpec> spec = jr->getSpec();
        if(spec) { spec = boost::shared_ptr<JoinSpec>(spec->clone()); }
        JoinRefN::Ptr njr(new JoinRefN(
                              visit(jr->getLeft()),
                              visit(jr->getRight()),
                              jr->getJoinType(),
                              jr->getIsNatural(),
                              spec));
        return njr;
    }
    Tuple& getTuple(SimpleTableN::Ptr t) {
        // FIXME: Switch to map and rethink the system
        typedef Tuples::iterator Iter;
        for(Iter i=_tuples.begin(),e=_tuples.end(); i != e; ++i) {
            if(i->node == t.get()) { return *i; }
        }
        throw std::logic_error("Not found in tuples");
    }
    TableRefN::Ptr lookup(TableRefN::Ptr t, int permutation) {
        // Don't handle non-simple right now.
        if(!t->isSimple()) { return TableRefN::Ptr(); }
        SimpleTableN::Ptr st(boost::dynamic_pointer_cast<SimpleTableN>(t));
        Tuple& tuple = getTuple(st);
        // Probably select one bit out of permutation, based on which
        // which subchunked table this is in the query.
        int i = _permutation & 1;
        if(i == 0) {
            return SimpleTableN::Ptr(new SimpleTableN(st->getDb(),
                                                      tuple.tables.front(),
                                                      st->getAlias()));
        } else {
            return SimpleTableN::Ptr(new SimpleTableN(st->getDb(),
                                                      tuple.tables.back(),
                                                      st->getAlias()));
        }
    }
#if 0
        // Idea: Make a TableRefN from each and add it to each list
        typedef std::list<std::string>::const_iterator Iter;
        Iter i=t.tables.begin();
        Iter e=t.tables.end();
        boost::shared_ptr<SimpleTableN> rn1;
        boost::shared_ptr<SimpleTableN> rn2;
        rn1.reset(new SimpleTableN(t.db, *i, t.alias));
        ++i;
        if(_firstSubChunkTable || (i==e))  {
            rn2.reset(new SimpleTableN(*rn1));
        } else {
            rn2.reset(new SimpleTableN(t.db, *i, t.alias));
            ++i;
            if(i != e) {
                throw std::logic_error("Unexpected third table entry");
            }
        }
        if(t.chunkLevel == 2) {
            _firstSubChunkTable = false;
        }
#endif

    TableRefnListPtr _tableRefnList;
    Tuples& _tuples;
    int _permutation;
};

////////////////////////////////////////////////////////////////////////
// TableStrategy public
////////////////////////////////////////////////////////////////////////
TableStrategy::TableStrategy(FromList const& f,
                             QueryContext& context)
    : _impl(new Impl(context)) {
    _import(f);
}

boost::shared_ptr<QueryMapping> TableStrategy::exportMapping() {
    boost::shared_ptr<QueryMapping> qm(new QueryMapping());

    LOGGER_DBG << __FILE__ ": _impl->chunkLevel : "
               << _impl->chunkLevel << std::endl;
    switch(_impl->chunkLevel) {
    case 0:
        break;
    case 1:
        LOGGER_DBG << __FILE__ ": calling  addChunkMap()"
                   << std::endl;
        qm->insertChunkEntry(CHUNKTAG);
        break;
    case 2:
        LOGGER_DBG << __FILE__": calling  addSubChunkMap()"
                   << std::endl;
        qm->insertChunkEntry(CHUNKTAG);
        qm->insertSubChunkEntry(SUBCHUNKTAG);
        updateMappingFromTuples(*qm, _impl->tuples);
        break;
    default:
        break;
    }
    return qm;

}
/// @return permuation count: 1 :singleton count (no subchunking)
///
int TableStrategy::getPermutationCount() const {
    return 1;
}

TableRefnListPtr TableStrategy::getPermutation(int permutation, TableRefnList const& tList) {
    TableRefnListPtr oList(new TableRefnList(tList.size()));
    std::transform(tList.begin(), tList.end(),
                   oList->begin(), computeTable(_impl->tuples, permutation));
    return oList;
}

void TableStrategy::setToPermutation(int permutation, TableRefnList& p) {
    // ignore permutation for now.
    inplaceComputeTable ict(_impl->tuples);
    std::for_each(p.begin(), p.end(), ict);
}

////////////////////////////////////////////////////////////////////////
// TableStrategy private
////////////////////////////////////////////////////////////////////////
void TableStrategy::_import(FromList const& f) {
    // Read into tuples. Why is the original structure insufficient?
    // Because I want to annotate! The annotations make subsequent
    // reasonaing and analysis possible. So the point will be to
    // populate a data structure of annotations.

    // Iterate over FromList elements
    TableRefnList const& tList = f.getTableRefnList();
    addTable a(_impl->tuples);
    std::for_each(tList.begin(), tList.end(), a);
    updateChunkLevel ucl(*_impl->context.metadata);
    std::for_each(_impl->tuples.begin(), _impl->tuples.end(), ucl);

    int chunkedTablesNumber = TableNamer::patchTuples(_impl->tuples);
    if(chunkedTablesNumber > 1) { _impl->chunkLevel = 2; }
    else if(chunkedTablesNumber == 1) { _impl->chunkLevel = 1; }
    else { _impl->chunkLevel = 0; }

    LOGGER_DBG << "SphericalBoxStrategy::_import() : _impl->chunkLevel : "
               << _impl->chunkLevel << std::endl;
    if(!_impl->context.metadata) {
        throw std::logic_error("Missing context.metadata");
    }
    _updateContext();
}

void TableStrategy::_updateContext() {
    // Patch context with mapping.
    if(_impl->context.queryMapping.get()) {
        _impl->context.queryMapping->update(*exportMapping());
    } else {
        _impl->context.queryMapping = exportMapping();
    }
}
}}} // lsst::qserv::master
