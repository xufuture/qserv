/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
  * @file FromList.cc
  *
  * @brief Implementation of FromList
  *
  * @author Daniel L. Wang, SLAC
  */
#include "query/FromList.h"
#include <iterator>

namespace qMaster=lsst::qserv::master;
using lsst::qserv::master::FromList;
using lsst::qserv::master::TableRefnList;
using lsst::qserv::master::TableRefnListPtr;

std::ostream&
qMaster::operator<<(std::ostream& os, FromList const& fl) {
    os << "FROM ";
    if(fl._tableRefns.get() && fl._tableRefns->size() > 0) {
        TableRefnList const& refList = *(fl._tableRefns);
        std::copy(refList.begin(), refList.end(),
                  std::ostream_iterator<TableRefN::Ptr>(os,", "));
    } else {
        os << "(empty)";
    }
    return os;
}

bool FromList::isJoin() const {    
    return (_tableRefns.get() && _tableRefns->size() > 1);
}

std::string
FromList::getGenerated() {
    QueryTemplate qt;
    renderTo(qt);
    return qt.dbgStr();
}

void
FromList::renderTo(qMaster::QueryTemplate& qt) const {
    if(_tableRefns.get() && _tableRefns->size() > 0) {
        TableRefnList const& refList = *_tableRefns;
        std::for_each(refList.begin(), refList.end(), TableRefN::render(qt));
    }
}

boost::shared_ptr<qMaster::FromList> FromList::copySyntax() {
    boost::shared_ptr<FromList> newL(new FromList(*this));
    // Shallow copy of expr list is okay.
    newL->_tableRefns.reset(new TableRefnList(*_tableRefns));
    // For the other fields, default-copied versions are okay.
    return newL;
}

boost::shared_ptr<qMaster::FromList> FromList::copyDeep() const {
    typedef TableRefnList::const_iterator Iter;
    boost::shared_ptr<FromList> newL(new FromList(*this));

    newL->_tableRefns.reset(new TableRefnList());

    for(Iter i=_tableRefns->begin(), e=_tableRefns->end(); i != e; ++ i) {
        newL->_tableRefns->push_back(*i);
    }
    // Okay to share columnRefMap: we do not own it.
    return newL;
}

typedef std::list<qMaster::TableRefnListPtr> ListList;
void permuteHelper(ListList::iterator i, ListList::iterator e, 
                   qMaster::TableRefnListPtr soFar, ListList& finals) {

    if(i == e) {
        //put sofar onto final
        finals.push_back(soFar);
    }
    TableRefnList& slotList = **i;
    ++i;
    if(slotList.size() > 1) {
        typedef TableRefnList::iterator Iter;
        for(Iter j=slotList.begin(), e2=slotList.end(); j != e2; ++j) {
            // FIXME: Clone the list?
            TableRefnListPtr nSoFar(new TableRefnList(*soFar));
            // FIXME: Clone the tableref?
            nSoFar->push_back(*j);
            permuteHelper(i, e, nSoFar, finals);
        }
    } else {
        soFar->push_back((**i).front());
        permuteHelper(i, e, soFar, finals);
    }
}
FromList::PtrList FromList::permute(TableRefN::Pfunc& f) {
    PtrList pList;
    ListList combos;
    typedef TableRefnList::const_iterator Iter;

    for(Iter i=_tableRefns->begin(), e=_tableRefns->end(); 
        i != e; ++i) {
        combos.push_back(TableRefnListPtr(new TableRefnList((**i).permute(f)))); 
    }
    ListList finals;
    permuteHelper(combos.begin(), combos.end(), TableRefnListPtr(new TableRefnList), finals);

    // Now compute a new FromList for each list in finals.        
    typedef ListList::iterator Liter;
    for(Liter i=finals.begin(), e=finals.end(); i != e; ++i) {
        pList.push_back(Ptr(new FromList(*i)));
    }

    return pList;
}
