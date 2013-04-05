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

#include "ChunkReducer.h"

#include <cstdio>
#include <stdexcept>
#include <string>

#include "boost/make_shared.hpp"

#include "Hash.h"

namespace fs = boost::filesystem;
namespace po = boost::program_options;

using std::runtime_error;
using std::snprintf;
using std::string;
using boost::make_shared;


namespace lsst { namespace qserv { namespace admin { namespace dupr {

ChunkReducer::ChunkReducer(po::variables_map const & vm) :
    _index(make_shared<ChunkIndex>()),
    _chunkId(-1),
    _numNodes(vm["out.num-nodes"].as<uint32_t>()),
    _outputDir(vm["out.dir"].as<string>()),
    _nonOverlap(vm["mr.block-size"].as<size_t>()*MiB),
    _selfOverlap(vm["mr.block-size"].as<size_t>()*MiB),
    _fullOverlap(vm["mr.block-size"].as<size_t>()*MiB)
{
    if (_numNodes == 0 || _numNodes > 99999u) {
        throw runtime_error("The --out.num-nodes option value must be "
                            "between 1 and 99999.");
    }
}

void ChunkReducer::reduce(ChunkReducer::RecordIter beg,
                          ChunkReducer::RecordIter end)
{
    if (beg == end) {
        return;
    }
    int32_t const chunkId = beg->key.chunkId;
    if (chunkId != _chunkId) {
        finish();
        _chunkId = chunkId;
        _makeFilePaths(chunkId);
    }
    // Store records and update statistics. Files are only created/
    // opened if there is data to write to them.
    for (; beg != end; ++beg) {
        _index->add(beg->key);
        if (beg->key.kind == ChunkLocation::NON_OVERLAP) {
            if (!_nonOverlap.isOpen()) {
                _nonOverlap.open(_nonOverlapPath, false);
            }
            _nonOverlap.append(beg->data, beg->size);
        } else {
            if (beg->key.kind == ChunkLocation::SELF_OVERLAP) {
               if (!_selfOverlap.isOpen()) {
                   _selfOverlap.open(_selfOverlapPath, false);
               }
               _selfOverlap.append(beg->data, beg->size);
            }
            // self-overlap locations are also full-overlap locations.
            if (!_fullOverlap.isOpen()) {
                _fullOverlap.open(_fullOverlapPath, false);
            }
            _fullOverlap.append(beg->data, beg->size);
        }
    }
}

void ChunkReducer::finish() {
    // Reset current chunk ID to an invalid ID and close all output files.
    _chunkId = -1;
    _nonOverlap.close();
    _selfOverlap.close();
    _fullOverlap.close();
}

void ChunkReducer::_makeFilePaths(int32_t chunkId) {
    fs::path p = _outputDir;
    if (_numNodes > 1) {
        // Files go into a node-specific sub-directory.
        char subdir[32];
        uint32_t node = mulveyHash(static_cast<uint32_t>(chunkId)) % _numNodes;
        snprintf(subdir, sizeof(subdir), "node_%05lu",
                 static_cast<unsigned long>(node));
        p /= subdir;
        fs::create_directory(p);
    }
    char file[32];
    snprintf(file, sizeof(file), "chunk_%ld.txt",
             static_cast<long>(chunkId));
    _nonOverlapPath = p / fs::path(file);
    snprintf(file, sizeof(file), "chunk_%ld_self.txt",
             static_cast<long>(chunkId));
    _selfOverlapPath = p / fs::path(file);
    snprintf(file, sizeof(file), "chunk_%ld_full.txt",
             static_cast<long>(chunkId));
    _fullOverlapPath = p / fs::path(file);
}

}}}} // namespace lsst::qserv::admin::dupr
