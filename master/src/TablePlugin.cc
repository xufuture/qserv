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
// TablePlugin replaces user query table names with substitutable
// names and maintains a list of tables that need to be substituted. 
#include "lsst/qserv/master/TablePlugin.h"
#include <string>

#include "lsst/qserv/master/QueryPlugin.h"

#include "lsst/qserv/master/SelectList.h"
#include "lsst/qserv/master/SelectStmt.h"
#include "lsst/qserv/master/FromList.h"
#include "lsst/qserv/master/WhereClause.h"
#include "lsst/qserv/master/FuncExpr.h"
#if 0
#include "lsst/qserv/master/QueryTemplate.h"
#include "lsst/qserv/master/ValueExpr.h"

#include "lsst/qserv/master/AggOp.h"
#endif

namespace qMaster=lsst::qserv::master;

namespace { // File-scope helpers
// Shared among plugins?
template <class C>
void printList(char const* label, C const& c) {
    typename C::const_iterator i; 
    std::cout << label << ": ";
    for(i = c.begin(); i != c.end(); ++i) {
        std::cout << **i << ", ";
    }
    std::cout << std::endl;
}
class ReverseAlias {
public:
    ReverseAlias() {}
    std::string const& get(std::string db, std::string table) {
        return _map[makeKey(db, table)];
    }
    void set(std::string const& db, std::string const& table, 
             std::string const& alias) {
        _map[makeKey(db, table)] = alias;
    }
    inline std::string makeKey(std::string db, std::string table) {
        std::stringstream ss;
        ss << db << "__" << table << "__";
        return ss.str();
    }
    std::map<std::string, std::string> _map;
};

class addMap {
public:
    explicit addMap(ReverseAlias& r) : _reverseAlias(r) {}
    void operator()(std::string const& alias, 
                    std::string const& db, std::string const& table) {
        std::cout << "set: " << alias << "->" 
                  << db << "." << table << std::endl;
        _reverseAlias.set(db, table, alias);
    }
    ReverseAlias& _reverseAlias;
};

class generateAlias {
public:
    generateAlias(int& seqN) : _seqN(seqN) {}
    std::string operator()() {
        std::stringstream ss;
        ss << "QST_" << ++_seqN << "_";
        return ss.str();
    }
    int& _seqN;
};

template <typename G, typename A>
class addAlias {
public:
    addAlias(G g, A a) : _generate(g), _addMap(a) {}
    void operator()(qMaster::TableRefN::Ptr t) {
        std::cout << "tableref:";
        t->putStream(std::cout);
        std::cout << std::endl;
        // If no alias, then add one. 
        std::string alias = t->getAlias();
        if(alias.empty()) {
            alias = _generate();
            t->setAlias(alias);
        }
        // Save ref
        _addMap(alias, t->getDb(), t->getTable());
    }
private:
    G _generate; // Functor that creates a new alias name
    A _addMap; // Functor that adds a new alias mapping for matchin
               // later clauses.
};
} // anonymous

namespace lsst { namespace qserv { namespace master {
// fixExprAlias is a functor that acts on ValueExpr objects and
// modifys them in-place, altering table names to use an aliased name
// that is mapped via ReverseAlias. 
// It does not add table qualifiers where none already exist, because
// there is no compelling reason to do so (yet).
class fixExprAlias {
public:
    explicit fixExprAlias(ReverseAlias& r) : _reverseAlias(r) {}
    void operator()(ValueExprPtr& vep) {
        if(!vep.get()) {
            return;
        }
        //std::cout << "fixing expr: " << *vep << std::endl;
        std::string newAlias;
        switch(vep->getType()) {
        case ValueExpr::COLUMNREF:
            // check columnref.
            _patchColumnRef(*vep->getColumnRef());
            break;
        case ValueExpr::FUNCTION:
        case ValueExpr::AGGFUNC:
            // recurse for func params (aggfunc is special case of function)
            _patchFuncExpr(*vep->getFuncExpr());
            break;
        case ValueExpr::STAR: 
            // Patch db/table name if applicable
            _patchStar(*vep);
            break;
        default: break;
        }
    }
private:
    inline void _patchColumnRef(ColumnRef& ref) {
        std::string newAlias = _getAlias(ref.db, ref.table);
        if(newAlias.empty()) { return; } //  Ignore if no replacement
                                         //  exists.
        // Eliminate db. Replace table with aliased table.
        ref.db.assign("");
        ref.table.assign(newAlias);
    }
    inline void _patchFuncExpr(FuncExpr& fe) {
        std::for_each(fe.params.begin(), fe.params.end(), 
                      fixExprAlias(_reverseAlias));
    }
    inline void _patchStar(ValueExpr& ve) {
        // TODO: No support for <db>.<table>.* in framework
        // Only <table>.* is supported.
        std::string newAlias = _getAlias("", ve.getTableStar());
        if(newAlias.empty()) { return; } //  Ignore if no replacement
                                         //  exists.
        else { ve.setTableStar(newAlias); }
    }


    inline std::string _getAlias(std::string const& db,
                                 std::string const& table) {
        //std::cout << "lookup: " << db << "." << table << std::endl;
        return _reverseAlias.get(db, table);
    }
    ReverseAlias& _reverseAlias;
};


////////////////////////////////////////////////////////////////////////
// TablePlugin declaration
////////////////////////////////////////////////////////////////////////
class TablePlugin : public lsst::qserv::master::QueryPlugin {
public:
    // Types
    typedef boost::shared_ptr<TablePlugin> Ptr;
    
    virtual ~TablePlugin() {}

    /// Prepare the plugin for a query
    virtual void prepare() {}

    /// Apply the plugin's actions to the parsed, but not planned query
    virtual void applyLogical(lsst::qserv::master::SelectStmt& stmt);

    /// Apply the plugins's actions to the concrete query plan.
    virtual void applyPhysical(lsst::qserv::master::QueryPlugin::Plan& p);
private:

};

////////////////////////////////////////////////////////////////////////
// TablePluginFactory declaration+implementation
////////////////////////////////////////////////////////////////////////
class TablePluginFactory : public lsst::qserv::master::QueryPlugin::Factory {
public:
    // Types
    typedef boost::shared_ptr<TablePluginFactory> Ptr;
    TablePluginFactory() {}
    virtual ~TablePluginFactory() {}

    virtual std::string getName() const { return "Table"; }
    virtual lsst::qserv::master::QueryPlugin::Ptr newInstance() {
        return lsst::qserv::master::QueryPlugin::Ptr(new TablePlugin());
    }
};

////////////////////////////////////////////////////////////////////////
// registarTablePlugin implementation
////////////////////////////////////////////////////////////////////////
// factory registration
void 
registerTablePlugin() {
    TablePluginFactory::Ptr f(new TablePluginFactory());
    QueryPlugin::registerClass(f);
}

////////////////////////////////////////////////////////////////////////
// TablePlugin implementation
////////////////////////////////////////////////////////////////////////
void TablePlugin::applyLogical(lsst::qserv::master::SelectStmt& stmt) {
    // Idea: Add aliases to all table references in the from-list (if
    // they don't exist already) and then patch the other clauses so
    // that they refer to the aliases.
    // The purpose of this is to confine table name references to the
    // from-list so that the later table-name substitution is confined
    // to modifying the from-list.
    FromList& fList = stmt.getFromList();
    std::cout << "TABLE:Logical:orig fromlist "
              << fList.getGenerated() << std::endl;
    TableRefnList& tList = fList.getTableRefnList();
    // For each tableref, modify to add alias.
    int seq=0;
    ReverseAlias reverseAlias;
    std::for_each(tList.begin(), tList.end(), 
                  addAlias<generateAlias,addMap>(generateAlias(seq), 
                                                 addMap(reverseAlias)));

    // Now snoop around the other clauses (SELECT, WHERE, etc. and
    // patch their table references)
    // select list
    SelectList& sList = stmt.getSelectList();
    ValueExprList& exprList = *sList.getValueExprList();
    std::for_each(exprList.begin(), exprList.end(), 
                  fixExprAlias(reverseAlias));
    // where
    WhereClause wClause = stmt.getWhereClause();
    WhereClause::ValueExprIter veI = wClause.vBegin();
    WhereClause::ValueExprIter veEnd = wClause.vEnd();
    std::for_each(veI, veEnd, fixExprAlias(reverseAlias));
    // Apply function using the iterator...
    //wClause.walk(fixExprAlias(reverseAlias));
    // order by
    // having
}

void
TablePlugin::applyPhysical(QueryPlugin::Plan& p) {
    // For each entry in original's SelectList, modify the SelectList
    // for the parallel and merge versions.
    // Set hasMerge to true if aggregation is detected.
    SelectList& oList = p.stmtOriginal.getSelectList();
    SelectList& pList = p.stmtParallel.getSelectList();
    SelectList& mList = p.stmtMerge.getSelectList();
    boost::shared_ptr<qMaster::ValueExprList> vlist;
    vlist = oList.getValueExprList();
    assert(vlist.get());
    
    // Idea: Rewrite table names in from-list of the parallel
    // query. This is sufficient because table aliases were added in
    // the logical plugin stage so that real table refs should only
    // exist in the from-list.
    FromList& fList = p.stmtParallel.getFromList();
    std::cout << "orig fromlist " << fList.getGenerated() << std::endl;
    // For each tableref, modify to replace tablename with substitutable.
    
#if 0
    // Clear out select lists, since we are rewriting them.
    pList.getValueExprList()->clear();
    mList.getValueExprList()->clear();
    

    AggOp::Mgr m; // Eventually, this can be shared?
    convertAgg<qMaster::ValueExprList> ca(*pList.getValueExprList(), 
                                          *mList.getValueExprList(),
                                          m);
    
    std::for_each(vlist->begin(), vlist->end(), ca);
    
    QueryTemplate qt;
    pList.renderTo(qt);
    std::cout << "pass: " << qt.dbgStr() << std::endl;
    qt.clear();
    mList.renderTo(qt);
    std::cout << "fixup: " << qt.dbgStr() << std::endl;

    // Also need to operate on GROUP BY.
#endif
}
}}} // namespace lsst::qserv::master
