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
  * @file FromList.cc
  *
  * @brief Implementation of FromList
  *
  * @author Daniel L. Wang, SLAC
  */
#include "query/FromList.h"

namespace qMaster=lsst::qserv::master;
using lsst::qserv::master::FromList;
using lsst::qserv::master::TableRefList;
using lsst::qserv::master::TableRefListPtr;

std::ostream&
qMaster::operator<<(std::ostream& os, FromList const& fl) {
    os << "FROM ";
    if(fl._tableRefs.get() && fl._tableRefs->size() > 0) {
        TableRefList const& refList = *(fl._tableRefs);
        std::copy(refList.begin(), refList.end(),
                  std::ostream_iterator<TableRef::Ptr>(os,", "));
    } else {
        os << "(empty)";
    }
    return os;
}

bool FromList::isJoin() const {
    return (_tableRefs.get() && _tableRefs->size() > 1);
}

std::string
FromList::getGenerated() {
    QueryTemplate qt;
    renderTo(qt);
    return qt.dbgStr();
}

void
FromList::renderTo(qMaster::QueryTemplate& qt) const {
    if(_tableRefs.get() && _tableRefs->size() > 0) {
        TableRefList const& refList = *_tableRefs;
        std::for_each(refList.begin(), refList.end(), TableRef::render(qt));
    }
}

boost::shared_ptr<qMaster::FromList> FromList::copySyntax() {
    boost::shared_ptr<FromList> newL(new FromList(*this));
    // Shallow copy of expr list is okay.
    newL->_tableRefs.reset(new TableRefList(*_tableRefs));
    // For the other fields, default-copied versions are okay.
    return newL;
}

boost::shared_ptr<qMaster::FromList> FromList::copyDeep() const {
    typedef TableRefList::const_iterator Iter;
    boost::shared_ptr<FromList> newL(new FromList(*this));

    newL->_tableRefs.reset(new TableRefList());

    for(Iter i=_tableRefs->begin(), e=_tableRefs->end(); i != e; ++ i) {
        newL->_tableRefs->push_back(*i);
    }
    // Okay to share columnRefMap: we do not own it.
    return newL;
}

typedef std::list<qMaster::TableRefListPtr> ListList;
void permuteHelper(ListList::iterator i, ListList::iterator e,
                   qMaster::TableRefListPtr soFar, ListList& finals) {

    if(i == e) {
        //put sofar onto final
        finals.push_back(soFar);
    }
    TableRefList& slotList = **i;
    ++i;
    if(slotList.size() > 1) {
        typedef TableRefList::iterator Iter;
        for(Iter j=slotList.begin(), e2=slotList.end(); j != e2; ++j) {
            // FIXME: Clone the list?
            TableRefListPtr nSoFar(new TableRefList(*soFar));
            // FIXME: Clone the tableref?
            nSoFar->push_back(*j);
            permuteHelper(i, e, nSoFar, finals);
        }
    } else {
        soFar->push_back((**i).front());
        permuteHelper(i, e, soFar, finals);
    }
}
FromList::PtrList FromList::permute(TableRef::Pfunc& f) {
    PtrList pList;
    ListList combos;
    typedef TableRefList::const_iterator Iter;

    for(Iter i=_tableRefs->begin(), e=_tableRefs->end();
        i != e; ++i) {
        combos.push_back(TableRefListPtr(new TableRefList((**i).permute(f))));
    }
    ListList finals;
    permuteHelper(combos.begin(), combos.end(), TableRefListPtr(new TableRefList), finals);

    // Now compute a new FromList for each list in finals.
    typedef ListList::iterator Liter;
    for(Liter i=finals.begin(), e=finals.end(); i != e; ++i) {
        pList.push_back(Ptr(new FromList(*i)));
    }

    return pList;
}
