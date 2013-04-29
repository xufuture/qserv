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

#ifndef LSST_QSERV_ADMIN_DUPR_CHUNKREDUCER_H
#define LSST_QSERV_ADMIN_DUPR_CHUNKREDUCER_H

#include <stdint.h>

#include "boost/filesystem/path.hpp"
#include "boost/program_options.hpp"
#include "boost/shared_ptr.hpp"

#include "Chunker.h"
#include "ChunkIndex.h"
#include "FileUtils.h"
#include "MapReduce.h"

namespace lsst { namespace qserv { namespace admin { namespace dupr {

/// Worker base class for the partitioner and duplicator which implements the
/// reduction related half of the map-reduce API.
///
/// The `reduce` function saves output records to files, each containing
/// data for a single chunk ID. Chunk IDs are assigned to down-stream nodes
/// by hashing, and the corresponding output files are created in node
/// specific sub-directories of the output directory.
///
/// The worker result is a ChunkIndex that tracks per chunk/sub-chunk record
/// counts.
class ChunkReducer : public WorkerBase<ChunkLocation, ChunkIndex> {
public:
    ChunkReducer(boost::program_options::variables_map const & vm);

    void reduce(RecordIter beg, RecordIter end);
    void finish();

    boost::shared_ptr<ChunkIndex> const result() { return _index; }

private:
    void _makeFilePaths(int32_t chunkId);

    boost::shared_ptr<ChunkIndex> _index;
    int32_t                       _chunkId;
    uint32_t                      _numNodes;
    std::string                   _prefix;
    boost::filesystem::path       _outputDir;
    boost::filesystem::path       _nonOverlapPath;
    boost::filesystem::path       _selfOverlapPath;
    boost::filesystem::path       _fullOverlapPath;
    BufferedAppender              _nonOverlap;
    BufferedAppender              _selfOverlap;
    BufferedAppender              _fullOverlap;
};

}}}} // namespace lsst::qserv::admin::dupr

#endif // LSST_QSERV_ADMIN_DUPR_CHUNKREDUCER_H
