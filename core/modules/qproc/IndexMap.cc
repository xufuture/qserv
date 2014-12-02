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
#include "global/intTypes.h"
#include "global/stringTypes.h"
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
    typedef std::map<std::string, MakeFunc> Map;
    Map fMap;
};

static FuncMap funcMap;
} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qproc {
typedef std::vector<boost::shared_ptr<Region> > RegionPtrVector;

boost::shared_ptr<Region> getRegion(query::Constraint const& c) {
    FuncMap::Map::const_iterator i = funcMap.fMap.find(c.name);
    if(i != funcMap.fMap.end()) {
        return i->second(c.params);
    }
    return boost::shared_ptr<Region>();
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

////////////////////////////////////////////////////////////////////////
// IndexMap::PartitioningMap definition and implementation
////////////////////////////////////////////////////////////////////////
class IndexMap::PartitioningMap {
public:
    explicit PartitioningMap(css::StripingParams const& sp) {
        sg::Angle overlap(0.000001); // Dummy.
        _chunker = boost::make_shared<sg::Chunker>(sp.stripes,
                                                   sp.subStripes);

    }
    /// @return un-canonicalized vector<SubChunks> of concatenated region
    /// results. Regions are assumed to be joined by implicit "OR" and not "AND"
    ///
    SubChunksVector getIntersect(RegionPtrVector const& rv) {
        SubChunksVector scv;
        for(RegionPtrVector::const_iterator i=rv.begin(), e=rv.end();
            i != e;
            ++i) {
            if(*i) {
                SubChunksVector area = getCoverage(**i);
                scv.insert(scv.end(), area.begin(), area.end());
            } else {
                throw Bug("Null region ptr in IndexMap");
            }
        }
        return scv;
    }

    inline SubChunksVector getCoverage(Region const& r) {
        return _chunker->getSubChunksIntersecting(r);
    }
    ChunkSpecVector getAllChunks() const {
        IntVector allChunks = _chunker->getAllChunks();
        ChunkSpecVector csv;
        csv.reserve(allChunks.size());
        for(IntVector::const_iterator i=allChunks.begin(), e=allChunks.end();
            i != e; ++i) {
            csv.push_back(ChunkSpec(*i, _chunker->getAllSubChunks(*i)));
        }
        return csv;
    }
private:
    boost::shared_ptr<sg::Chunker> _chunker;
};

////////////////////////////////////////////////////////////////////////
// IndexMap implementation
////////////////////////////////////////////////////////////////////////
IndexMap::IndexMap(css::StripingParams const& sp,
                   boost::shared_ptr<SecondaryIndex> si)
    : _pm(boost::make_shared<PartitioningMap>(sp)),
      _si(si) {

}

ChunkSpecVector IndexMap::getAll() {
    return _pm->getAllChunks();
}

ChunkSpecVector IndexMap::getIntersect(query::ConstraintVector const& cv) {
    // Index lookups
    query::ConstraintVector indexConstraints;
    //    std::copy_if(cv.begin(), cv.end(), std::back_inserter(indexConstraints),
    //                 isIndex);
    // copy_if not available before c++11
    std::copy(cv.begin(), cv.end(), std::back_inserter(indexConstraints));
    query::ConstraintVector::const_iterator newEnd;
    newEnd = std::remove_if(indexConstraints.begin(), indexConstraints.end(),
                            isNotIndex);
    indexConstraints.resize(newEnd - indexConstraints.begin());
    ChunkSpecVector indexSpecs;
    if(!indexConstraints.empty()) {
        if(!_si) {
            throw Bug("Invalid SecondaryIndex in IndexMap. Check IndexMap(...)");
        }
        indexSpecs = _si->lookup(indexConstraints);
    }

    // Spatial area lookups
    if(indexConstraints.size() == cv.size()) {
        if(indexConstraints.empty()) { // No constraints -> full-sky
            return _pm->getAllChunks();
        } else {
            // No spatial constraints?
            return indexSpecs;
        }
    } else {
        // Index and spatial lookup are supported in AND format only right now.
        // If both exist, compute the AND.
        RegionPtrVector rv;
        std::transform(cv.begin(), cv.end(), std::back_inserter(rv), getRegion);

        SubChunksVector scv = _pm->getIntersect(rv);

        ChunkSpecVector csv;
        std::transform(scv.begin(), scv.end(),
                       std::back_inserter(csv), convertSgSubChunks);
        if(!indexSpecs.empty()) {
            intersectSorted(csv, indexSpecs); // performs "AND"
        }
        return csv;
    }
}


}}} // namespace lsst::qserv::qproc


