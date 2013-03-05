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

/// \file
/// \brief The Qserv HTM indexer.

#include <cstdio>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "boost/filesystem.hpp"
#include "boost/make_shared.hpp"
#include "boost/program_options.hpp"
#include "boost/shared_ptr.hpp"
#include "boost/timer/timer.hpp"

#include "CmdLineUtils.h"
#include "Csv.h"
#include "FileUtils.h"
#include "Geometry.h"
#include "Hash.h"
#include "HtmIndex.h"
#include "MapReduce.h"

using std::cerr;
using std::cout;
using std::endl;
using std::exception;
using std::pair;
using std::runtime_error;
using std::snprintf;
using std::string;
using std::vector;

using boost::make_shared;
using boost::shared_ptr;
using boost::timer::cpu_timer;

namespace fs = boost::filesystem;
namespace po = boost::program_options;


namespace lsst { namespace qserv { namespace admin { namespace dupr {

/// An ID extracted from a CSV record, along with the HTM ID
/// of the associated partitioning position.
struct Key {
    int64_t  id;
    uint32_t htmId;
};


/// Minimize the size of `Record<Key>` by flattening `Key` fields into
/// the record type.
template <> struct Record<Key> {
    int64_t  id;
    uint32_t htmId;
    uint32_t size;
    char *   data;

    Record() : id(0), htmId(0), size(0), data(0) { }

    explicit Record(Key const & k) :
        id(k.id), htmId(k.htmId), size(0), data(0) { }

    /// Hash records by HTM ID.
    uint32_t hash() const { return mulveyHash(htmId); }

    /// Order records by HTM ID.
    bool operator<(Record const & r) const { return htmId < r.htmId; }
};


/// Map-reduce worker class for HTM indexing.
///
/// The `map` function extracts a record ID and computes an HTM ID for each
/// input record.
///
/// The `reduce` function saves output records and record IDs to files, each
/// containing data for a single HTM ID. Additionally, each HTM ID is assigned
/// to a down-stream node by hashing, and the corresponding output files are
/// created in a node specific sub-directory of the output directory.
///
/// A worker's result is an HtmIndex that contains the total record count
/// and size for each HTM ID seen by that worker.
class Worker : public WorkerBase<Key, HtmIndex> {
public:
    Worker(po::variables_map const & vm);

    // map-reduce API
    void map(char const * beg, char const * end, Silo & silo);
    void reduce(RecordIter beg, RecordIter end);
    void finish();

    shared_ptr<HtmIndex> const result() { return _index; }

    static void defineOptions(po::options_description & opts);

private:
    void _openFiles(uint32_t htmId);

    csv::Editor          _editor;
    int                  _idField;
    int                  _raField;
    int                  _decField;
    int                  _level;
    shared_ptr<HtmIndex> _index;
    HtmIndex::Triangle   _triangle;
    uint32_t             _numNodes;
    fs::path             _outputDir;
    BufferedAppender     _records;
    BufferedAppender     _ids;
};

Worker::Worker(po::variables_map const & vm) :
    _editor(vm),
    _idField(-1),
    _raField(-1),
    _decField(-1),
    _level(vm["level"].as<int>()),
    _index(make_shared<HtmIndex>(_level)),
    _triangle(),
    _numNodes(vm["out.num-nodes"].as<uint32_t>()),
    _outputDir(vm["out.dir"].as<string>()),
    _records(vm["mr.block-size"].as<size_t>()*MiB),
    _ids(vm["mr.block-size"].as<size_t>()*MiB)
{
    if (_numNodes == 0 || _numNodes > 99999u) {
        throw runtime_error("The --out.num-nodes option value must be "
                            "between 1 and 99999.");
    }
    // Map field names of interest to field indexes.
    if (vm.count("part.id") == 0) {
        throw runtime_error("The --part.id option was not specified.");
    }
    string s = vm["part.id"].as<string>();
    _idField = _editor.getFieldIndex(s);
    if (_idField == -1) {
        throw runtime_error("--part.id=\"" + s + "\" is not a valid input "
                            "field name.");
    }
    s = vm["part.ra"].as<string>();
    _raField = _editor.getFieldIndex(s);
    if (_raField < 0) {
        throw runtime_error("--part.ra=\"" + s + "\" is not a valid "
                            "input field name.");
    }
    s = vm["part.decl"].as<string>();
    _decField = _editor.getFieldIndex(s);
    if (_decField < 0) {
        throw runtime_error("--part.decl=\"" + s + "\" is not a valid "
                            "input field names.");
    }
}

void Worker::map(char const * beg, char const * end, Worker::Silo & silo) {
    Key k;
    pair<double, double> sc;
    while (beg < end) {
        // Parse input line.
        beg = _editor.readRecord(beg, end);
        // Extract ID field and partitioning position.
        k.id = _editor.get<int64_t>(_idField);
        sc.first = _editor.get<double>(_raField);
        sc.second = _editor.get<double>(_decField);
        // Compute HTM ID of partitioning position and store output record.
        k.htmId = htmId(cartesian(sc), _level);
        silo.add(k, _editor);
    }
}

void Worker::reduce(Worker::RecordIter beg, Worker::RecordIter end) {
    // Records in [beg, end) all have the same HTM ID.
    if (beg == end) {
        return;
    }
    uint32_t const htmId = beg->htmId;
    if (htmId != _triangle.id) {
        if (_triangle.id != 0) {
            // Update index with statistics for the current HTM ID, if any.
            _index->merge(_triangle);
        }
        // Clear HTM triangle statistics.
        _triangle.numRecords = 0;
        _triangle.recordSize = 0;
        // Open output files for the new HTM ID.
        _triangle.id = htmId;
        _openFiles(htmId);
    }
    // Store records and their IDs.
    for (; beg != end; ++beg) {
        uint8_t buf[8];
        _triangle.numRecords += 1;
        _triangle.recordSize += beg->size;
        _records.append(beg->data, beg->size);
        encode(buf, static_cast<uint64_t>(beg->id));
        _ids.append(buf, sizeof(buf));
    }
}

void Worker::finish() {
    if (_triangle.id != 0) {
        // Update index with statistics for the current HTM ID, if any.
        _index->merge(_triangle);
    }
    // Reset HTM triangle statistics and close currently open files.
    _triangle.id = 0;
    _triangle.numRecords = 0;
    _triangle.recordSize = 0;
    _records.close();
    _ids.close();
}

void Worker::defineOptions(po::options_description & opts) {
    po::options_description indexing("\\_______________ HTM indexing", 80);
    indexing.add_options()
        ("level", po::value<int>()->default_value(8),
         "HTM index subdivision level.")
        ("incremental", po::bool_switch(),
         "Allow incrementally adding to an existing index.");
    po::options_description part("\\_______________ Partitioning", 80);
    part.add_options()
        ("part.id", po::value<string>(),
         "The name of the record ID input field.")
        ("part.ra", po::value<string>()->default_value("ra"),
         "The partitioning right ascension field name.")
        ("part.decl", po::value<string>()->default_value("decl"),
         "The partitioning declination field name.");
    po::options_description output("\\_____________________ Output", 80);
    output.add_options()
        ("out.dir", po::value<string>()->default_value("index/"),
         "The output file directory. Unless running incrementally, this "
         "directory is not allowed to exist.")
        ("out.num-nodes", po::value<uint32_t>()->default_value(1u),
         "The number of duplicator nodes that will be processing the output "
         "files. If this is more than 1, then output files are assigned to "
         "duplicators by hashing and are placed into a sub-directory of "
         "out.dir named node_XXXXX, where XXXXX is a logical ID for the "
         "duplicator node between 0 and out.num-nodes - 1.");
    opts.add(indexing).add(part).add(output);
    csv::Editor::defineOptions(opts);
}

void Worker::_openFiles(uint32_t htmId) {
    fs::path p = _outputDir;
    if (_numNodes > 1) {
        // Files go into a node-specific sub-directory.
        char subdir[32];
        uint32_t node = mulveyHash(htmId) % _numNodes;
        snprintf(subdir, sizeof(subdir), "node_%05lu",
                 static_cast<unsigned long>(node));
        p /= subdir;
        fs::create_directory(p);
    }
    char file[32];
    snprintf(file, sizeof(file), "htm_%lx.txt",
             static_cast<unsigned long>(htmId));
    _records.open(p / fs::path(file), false);
    snprintf(file, sizeof(file), "htm_%lx.ids",
             static_cast<unsigned long>(htmId));
    _ids.open(p / fs::path(file), false);
}


typedef Job<Worker> HtmIndexJob;

}}}} // namespace lsst::qserv::admin::dupr


static char const * help =
    "The Qserv HTM indexer indexes one or more input CSV files in\n"
    "preparation for the Qserv spatial data duplicator.\n"
    "\n"
    "An index can be built incrementally by running the indexer with\n"
    "disjoint input file sets and the same output directory. Beware -\n"
    "the output CSV format, HTM subdivision-level, and duplicator\n"
    "node count MUST be identical between runs. Additionally, only one\n"
    "indexer process should use a given output directory at a time.\n"
    "If any of these conditions are not met, then the resulting\n"
    "index will be corrupt and/or useless.\n";

int main(int argc, char const * const * argv) {
    using lsst::qserv::admin::dupr::HtmIndex;
    using lsst::qserv::admin::dupr::HtmIndexJob;
    using lsst::qserv::admin::dupr::parseCommandLine;

    try {
        // Get job options and include some command-line only options.
        po::options_description options;
        HtmIndexJob::defineOptions(options);
        po::variables_map vm;
        parseCommandLine(vm, options, argc, argv, help);
        // Create output directory
        fs::path outDir = fs::system_complete(fs::path(vm["out.dir"].as<string>()));
        if (outDir.filename() == ".") {
            // create_directories returns false for "does_not_exist/", even
            // when "does_not_exist" must be created. This is because the
            // trailing slash causes the last path component to be ".", which
            // exists once it is iterated to.
            outDir.remove_filename();
        }
        po::variable_value v = vm["out.dir"];
        v.value() = outDir.native();
        static_cast<std::map<string, po::variable_value> >(vm)["out.dir"] = v;
        if (fs::create_directories(outDir) == false &&
            !vm["incremental"].as<bool>()) {
            cerr << "The output directory --out.dir=" << outDir.native()
                 << " already exists - please choose another." << endl;
            return EXIT_FAILURE;
        }
        // Launch the HTM indexing job.
        cpu_timer t;
        HtmIndexJob job(vm);
        shared_ptr<HtmIndex> index = job.run();
        // Write out results.
        if (!index->empty()) {
            index->write(outDir / fs::path("htm_index.bin"), false);
        }
        if (vm.count("verbose") != 0) {
            cerr << "run-time: " << t.format() << endl;
            cout << *index << endl;
        }
    } catch (exception const & ex) {
        cerr << ex.what() << endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
