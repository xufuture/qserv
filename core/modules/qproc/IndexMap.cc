// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014-2015 AURA/LSST.
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
  * @brief Implementation of IndexMap
  *
  * @author Daniel L. Wang, SLAC
  */

#include "qproc/IndexMap.h"

// System headers
#include <algorithm>
#include <cassert>
#include <iterator>
#include <stdexcept>
#include <set>
#include <vector>

// Third-party headers
#include "boost/lexical_cast.hpp"
// LSST headers
#include "sg/Chunker.h"
// Qserv headers
#include "global/stringTypes.h"
//#include "qproc/fakeGeometry.h"

#include "qproc/geomAdapter.h"
#include "qproc/SecondaryIndex.h"
#include "query/Constraint.h"

using lsst::qserv::StringVector;
using lsst::sg::Region;
using lsst::sg::Box;
using lsst::sg::Circle;
using lsst::sg::Ellipse;
using lsst::sg::ConvexPolygon;
using lsst::sg::SubChunks;
using lsst::qserv::qproc::Coordinate;

// Tmp:
typedef std::vector<SubChunks> SubChunksVector;

namespace { // File-scope helpers
template <typename T>
std::vector<T> convertVec(StringVector const& v) {
    std::vector<T> out;
    std::transform(v.begin(), v.end(), std::back_inserter(out),
                   boost::lexical_cast<T, std::string>);
    return out;
}
template <typename T>
boost::shared_ptr<Region> make(StringVector const& v) {
        return boost::shared_ptr<Region>(new T(convertVec<Coordinate>(v)));
}
template <>
boost::shared_ptr<Region> make<Box>(StringVector const& v) {
    return lsst::qserv::qproc::getBoxFromParams(convertVec<Coordinate>(v));
}
template <>
boost::shared_ptr<Region> make<Circle>(StringVector const& v) {
    return lsst::qserv::qproc::getCircleFromParams(convertVec<Coordinate>(v));
}
template <>
boost::shared_ptr<Region> make<Ellipse>(StringVector const& v) {
    return lsst::qserv::qproc::getEllipseFromParams(convertVec<Coordinate>(v));
}
template <>
boost::shared_ptr<Region> make<ConvexPolygon>(StringVector const& v) {
    return lsst::qserv::qproc::getConvexPolyFromParams(convertVec<Coordinate>(v));
}

typedef boost::shared_ptr<Region>(*MakeFunc)(StringVector const& v);

struct FuncMap {
    FuncMap() {
        fMap["box"] = make<Box>;
        fMap["circle"] = make<Circle>;
        fMap["ellipse"] = make<Ellipse>;
        fMap["poly"] = make<ConvexPolygon>;
        fMap["qserv_areaspec_box"] = make<Box>;
        fMap["qserv_areaspec_circle"] = make<Circle>;
        fMap["qserv_areaspec_ellipse"] = make<Ellipse>;
        fMap["qserv_areaspec_poly"] = make<ConvexPolygon>;
    }
    std::map<std::string, MakeFunc> fMap;
};

static FuncMap funcMap;
} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qproc {
typedef std::vector<boost::shared_ptr<Region> > RegionPtrVector;

boost::shared_ptr<Region> getRegion(query::Constraint const& c) {
    return funcMap.fMap[c.name](c.params);
}

ChunkSpec convertSgSubChunks(SubChunks const& sc) {
    ChunkSpec cs;
    cs.chunkId = sc.chunkId;
    cs.subChunks.resize(sc.subChunkIds.size());
    std::copy(sc.subChunkIds.begin(), sc.subChunkIds.end(),
              cs.subChunks.begin());
    return cs;
}

bool isIndex(query::Constraint const& c) {
    return c.name == "sIndex";
}
bool isNotIndex(query::Constraint const& c) {
    return c.name != "sIndex";
}

#if 0 // Duplicates functionality in ChunkSpec
class ChunkSpecSet {
public:
    typedef std::map<int, ChunkSpec> Map;

    ChunkSpecSet(ChunkSpecVector const& a) {
        ChunkSpecVector::const_iterator i, e;
        for(i = a.begin(), e = a.end(); i != e; ++i) {
            ChunkSpec const& cs = *i;
            Map::iterator existing = _map.find(cs.chunkId);
            if(existing != _map.end()) {
                existing->second.mergeUnion(cs);// _union(existing->second, cs.subChunks);
            } else {
                _map.insert(Map::value_type(cs.chunkId, cs));
            }
        }
    }
    /// Restrict the existing ChunkSpecSet by r, that is, the ChunkSpecSet is
    /// replaced by the intersection of the existing with r. Assume that r
    /// contains no duplicates in chunkId.
    void restrict(ChunkSpecVector const& r) {
        Map both;
        for(ChunkSpecVector::const_iterator i=r.begin(), e=r.end();
            i != e; ++i) {
            ChunkSpec const& cs = *i;
            Map::iterator existing = _map.find(cs.chunkId);
            if(existing != _map.end()) {
                both[cs.chunkId] = existing->second.intersect(cs);
            } else { // No match? Leave it alone
            }
            _map.swap(both);
        }
    }
    boost::shared_ptr<ChunkSpecVector> getSet() const {
        boost::shared_ptr<ChunkSpecVector> r
            = boost::make_shared<ChunkSpecVector>();
        for(Map::const_iterator i=_map.begin(), e=_map.end();
            i != e; ++i) {
            r->push_back(i->second);
        }
        return r;
    }
private:
    void _union(std::vector<int>& left, std::vector<int> const& right) {
        // left <- left union right
        std::vector<int> r1(right.begin(), right.end());
        std::vector<int> l1;
        l1.swap(left);
        std::sort(r1.begin(), r1.end());
        std::set_union(l1.begin(), l1.end(), r1.begin(), r1.end(),
                       std::back_inserter<std::vector<int> >(left));
    }
    ///
    void _intersect(std::vector<int>& left, std::vector<int> const& right) {
        // left = left intersect right

    }
    Map _map;

};
#endif

ChunkSpecVector IndexMap::getIntersect(query::ConstraintVector const& cv) {
    // Index lookups
    query::ConstraintVector indexConstraints;
    //    std::copy_if(cv.begin(), cv.end(), std::back_inserter(indexConstraints),
    //                 isIndex);
    // copy_if not available before c++11
    std::copy(cv.begin(), cv.end(), std::back_inserter(indexConstraints));
    std::remove_if(indexConstraints.begin(), indexConstraints.end(),
                   isNotIndex);
    ChunkSpecVector indexSpecs;
    if(!indexConstraints.empty()) {
        if(!_si) {
            throw Bug("Invalid SecondaryIndex in IndexMap. Check IndexMap(...)");
        }
        indexSpecs = _si->lookup(indexConstraints);
    }

    // Spatial area lookups
    if(indexConstraints.size() == cv.size()) {
        // No spatial constraints?
        return indexSpecs;
    } else {
        // Index and spatial lookup are supported in AND format only right now.
        // If both exist, compute the AND.
        RegionPtrVector rv;
        std::transform(cv.begin(), cv.end(), std::back_inserter(rv), getRegion);
        SubChunksVector scv; // = _pm->getIntersect(rv);

        throw "FIXME: do partitioningmap!";
        ChunkSpecVector csv;
        std::transform(scv.begin(), scv.end(),
                       std::back_inserter(csv), convertSgSubChunks);
        intersectSorted(csv, indexSpecs);
        return csv;
    }
}


}}} // namespace lsst::qserv::qproc


