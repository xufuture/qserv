#include "Options.h"

#include <iostream>

#include "boost/program_options.hpp"

#include "Htm.h"

using std::cerr;
using std::cout;
using std::endl;
using std::find;
using std::pair;
using std::string;
using std::vector;

namespace po = boost::program_options;


namespace dupr {

namespace {

typedef vector<string>::const_iterator StringIter;

// Return a copy of s with leading and trailing whitespace removed.
string const trim(string const & s) {
    static string const ws(" \t\n\r\f");
    size_t start = s.find_first_not_of(ws);
    if (start == string::npos) {
        return string();
    }
    size_t end = s.find_last_not_of(ws);
    return s.substr(start, (end - start) + 1);
}

// Split s into pieces separated by commas, trim whitespace,
// and return the pieces as a vector.
vector<string> split(string const & s) {
    vector<string> pieces;
    if (s.empty()) {
        return pieces;
    }
    size_t i = 0;
    while (true) {
        size_t next = s.find(',', i);
        if (next == string::npos) {
            pieces.push_back(trim(s.substr(i)));
            break;
        }
        pieces.push_back(trim(s.substr(i, next - i)));
        i = next + 1;
    }
    return pieces;
}

int getFieldIndex(
    Options const & opts,
    string const & field,
    string const & optionName,
    bool required=true
) {
    StringIter i = find(opts.fields.begin(), opts.fields.end(), field);
    if (i == opts.fields.end()) {
        if (!required) {
            return -1;
        }
        cerr << field + " field specified via --" << optionName
             << " does not exist" << endl;
        exit(EXIT_FAILURE);
    }
    return static_cast<int>(i - opts.fields.begin());
}

int getFieldIndex(
    po::variables_map const & vm,
    Options const & opts,
    string const & optionName,
    bool required=true
) {
    string field = vm[optionName].as<string>();
    return getFieldIndex(opts, field, optionName, required);
}

FieldPair const parseFieldPair(
    Options const & opts,
    string const & value,
    string const & optionName
) {
    vector<string> v = split(value);
    if (v.size() != 2 || v[0].empty() || v[1].empty() || v[0] == v[1]) {
        cerr << "--" << optionName << "must consist of a comma separated "
                "pair of distinct, non-empty field names" << endl;
        exit(EXIT_FAILURE);
    }
    FieldPair p;
    p.first = getFieldIndex(opts, v[0], optionName);
    p.second = getFieldIndex(opts, v[1], optionName);
    return p;
}

FieldPair const parseFieldPair(
    po::variables_map const & vm,
    Options const & opts,
    string const & optionName
) {
    if (vm.count(optionName) != 1) {
        cerr << "--" << optionName << " not specified"<< endl;
        exit(EXIT_FAILURE);
    }
    return parseFieldPair(opts, vm[optionName].as<string>(), optionName);
}

// -- Options common to all utilities --------

void buildCommonOptions(po::options_description & options) {
    po::options_description general("General options", 80);
    general.add_options()
        ("help", po::bool_switch(),
            "Print usage help.")
        ("num-threads", po::value<int>()->default_value(4),
            "Number of threads to launch.")
        ("fields", po::value<string>(),
            "Comma separated list of field names in input file(s) or index.")
        ("delimiter", po::value<char>()->default_value(','),
            "CSV delimiter character.")
        ("partitioned-by", po::value<string>(),
            "Name of partitioning right ascension and declination "
            "fields, separated by a comma. Must be specified exactly once.")
        ("block-size", po::value<size_t>()->default_value(32),
            "IO block size in MiB");
    options.add(general);
}

void validateAndStoreCommonOptions(po::variables_map const & vm, Options & opts) {
    opts.numThreads = vm["num-threads"].as<int>();
    if (opts.numThreads < 1 || opts.numThreads > 100) {
        cerr << "--num-threads value must be between 1 and 100" << endl;
        exit(EXIT_FAILURE);
    }
    if (vm.count("fields") != 1) {
        cerr << "--fields not specified"<< endl;
        exit(EXIT_FAILURE);
    }
    opts.fields = split(vm["fields"].as<string>());
    opts.delimiter = vm["delimiter"].as<char>();
    if (opts.delimiter == '\\' || opts.delimiter == '"' || opts.delimiter == '\n') {
        cerr << "--delimiter cannot be '\\' '\\n' or '\"'"<< endl;
        exit(EXIT_FAILURE);
    }
    opts.partitionPos = parseFieldPair(vm, opts, "partitioned-by");
    opts.blockSize = vm["block-size"].as<size_t>()*1024*1024;
    if (opts.blockSize < 2*1024*1024) {
        cerr << "--block-size must be at least 2 (MiB)" << endl;
        exit(EXIT_FAILURE);
    }
}


// -- Options for CSV input files --------

void buildCsvOptions(po::options_description & options) {
    po::options_description input("Input options", 80);
    input.add_options()
        ("input-csv", po::value<vector<string> >(),
            "Input CSV file(s). Must be specified at least once.");
    options.add(input);
}

void validateAndStoreCsvOptions(po::variables_map const & vm, Options & opts) {
    if (vm.count("input-csv") == 0) {
        cerr << "No inputs specified" << endl;
        exit(EXIT_FAILURE);
    }
    opts.inputFiles = vm["input-csv"].as<vector<string> >();
}


// -- Options for indexing CSV input files --------

void buildIndexingOptions(po::options_description & options) {
    po::options_description indexing("Indexing options", 80);
    indexing.add_options()
        ("merge-arity", po::value<size_t>()->default_value(32),
            "Number of input blocks to merge at a time.")
        ("htm-level", po::value<int>()->default_value(8),
            "HTM subdivision level.")
        ("primary-key", po::value<string>(),
            "Name of primary record ID field, e.g. sourceId in the "
            "Source table. Must be specified exactly once.");
    options.add(indexing);
}

void validateAndStoreIndexingOptions(po::variables_map const & vm, Options & opts) {
    opts.k = vm["merge-arity"].as<size_t>();
    if (opts.k < 2) {
        cerr << "--merge-arity must be at least 2" << endl;
        exit(EXIT_FAILURE);
    }
    opts.htmLevel = vm["htm-level"].as<int>();
    if (opts.htmLevel < 0 || opts.htmLevel > HTM_MAX_LEVEL) {
        cerr << "--htm-level value must be an integer between 0 and "
             << HTM_MAX_LEVEL << endl;
        exit(EXIT_FAILURE);
    }
    opts.pkField = getFieldIndex(vm, opts, "primary-key");
}


// -- Options for partitioning CSV input files or duplicated data --------

void buildPartitioningOptions(po::options_description & options) {
    po::options_description partitioning("Partitioning options", 80);
    partitioning.add_options()
        ("num-stripes", po::value<int32_t>()->default_value(18),
            "The number of declination stripes to divide the sky into.")
        ("num-sub-stripes-per-stripe", po::value<int32_t>()->default_value(100),
            "The number of sub-stripes to divide each stripe into.")
        ("overlap", po::value<double>()->default_value(0.01667),
            "Chunk/sub-chunk overlap radius (deg).")
        ("chunk-id-field", po::value<string>()->default_value("chunkId"),
            "Name of chunk ID field.")
        ("sub-chunk-id-field", po::value<string>()->default_value("subChunkId"),
            "Name of sub-chunk ID field.")
        ("secondary-sort-field", po::value<string>(),
            "Name of secondary sort field. Note that the primary sorting "
            "field for a chunk is always the sub-chunk ID, and that the "
            "secondary sort field defaults to the the value of --primary-key.");
    options.add(partitioning);
}

void validateAndStorePartitioningOptions(po::variables_map const & vm, Options & opts) {
    opts.numStripes = vm["num-stripes"].as<int32_t>();
    if (opts.numStripes < 1 || opts.numStripes >= 32768) {
        cerr << "value of --num-stripes must lie in range [1,32768)" << endl;
    }
    opts.numSubStripesPerStripe = vm["num-sub-stripes-per-stripe"].as<int32_t>();
    if (opts.numSubStripesPerStripe < 1 || opts.numSubStripesPerStripe >= 32768) {
        cerr << "value of --num-sub-stripes-per-stripe must lie in range [1,32768)" << endl;
    }
    opts.overlap = vm["overlap"].as<double>();
    if (opts.overlap < 0.0 || opts.overlap > 10.0) {
        cerr << "value of --overlap must be in range [0,10] deg" << endl;
    }
    opts.chunkIdField = getFieldIndex(vm, opts, "chunk-id-field", false);
    opts.subChunkIdField = getFieldIndex(vm, opts, "sub-chunk-id-field", false);
    if (vm.count("secondary-sort-field") == 0) {
        opts.secondarySortField = getFieldIndex(vm, opts, "primary-key");
    } else {
        opts.secondarySortField = getFieldIndex(vm, opts, "secondary-sort-field");
    }
}


// -- Options for duplicating data --------

void buildDuplicationOptions(po::options_description & options) {
    po::options_description duplication("Duplication options", 80);
    duplication.add_options()
        ("position", po::value<vector<string> >(),
            "Name of a right ascension and declination field, separated by a "
            "comma. The duplicator will remap these along with the partitioning "
            "position. May be specified any number of times.")
        ("primary-key", po::value<string>(),
            "Name of unique record ID field, e.g. sourceId in the "
            "Source table. Must be specified exactly once.")
        ("foreign-key", po::value<string>(),
            "Name of a foreign unique ID field (e.g. objectId in the Source "
            "table. Optional, and must be accompanied by --foreign-key-index "
            "if it is specified.")
        ("foreign-key-index", po::value<string>(),
            "Duplication index directory for foreign key values. "
            "Ignored unless --foreign-key is specified.")
        ("ra-min", po::value<double>()->default_value(0.0),
            "Minimum right ascension bound (deg) for the "
            "duplication region.")
        ("ra-max", po::value<double>()->default_value(360.0),
            "Maximum right ascension bound (deg) for the "
            "duplication region.")
        ("dec-min", po::value<double>()->default_value(-90.0),
            "Minimum declination bound (deg) for the "
            "duplication region.")
        ("dec-max", po::value<double>()->default_value(90.0),
            "Maximum declination bound (deg) for the "
            "duplication region.")
        ("node", po::value<uint32_t>()->default_value(0u),
            "The node to generate data for. Must be less "
            "than --num-nodes. Ignored if --chunk-id is specified.")
        ("num-nodes", po::value<uint32_t>()->default_value(1u),
            "The total number of qserv worker nodes. "
            "Ignored if --chunk-id is specified.")
        ("chunk-id", po::value<std::vector<int32_t> >(),
            "A specific chunk ID to generate data for. Can be specified "
            "any number of times, or not at all. If specified, data will "
            "be generated for the corresponding chunk(s), regardless of the "
            "the duplication region (--ra-bounds/--dec-bounds) and node "
            "(--node-number/--num-nodes).")
        ("hash-chunks", po::bool_switch(),
            "Assign a chunk to a node when the node number equals "
            "the hash of the chunk ID modulo the number of nodes. Otherwise "
            "chunks are assigned to nodes in round-robin fashion. Ignored "
            "if --chunk-id is specified.");
    options.add(duplication);
    po::options_description input("Input options", 80);
    input.add_options()
        ("index-dir", po::value<string>(),
            "Input index directory. Must be specified exactly once.");
    options.add(input);
}

void validateAndStoreDuplicationOptions(po::variables_map const & vm, Options & opts) {
    opts.indexDir = vm["index-dir"].as<string>();
    if (vm.count("position") > 0) {
        vector<string> p = vm["position"].as<vector<string> >();
        for (StringIter i = p.begin(), e = p.end(); i != e; ++i) {
            FieldPair p = parseFieldPair(opts, *i, "position");
            if (p.first == opts.partitionPos.first ||
                p.first == opts.partitionPos.second ||
                p.second == opts.partitionPos.first ||
                p.second == opts.partitionPos.second) {
                cerr << "--position field(s) conflict with --partitioned-by" << endl;
                exit(EXIT_FAILURE);
            }
            typedef vector<FieldPair>::const_iterator FpIter;
            for (FpIter f = opts.positions.begin(), fe = opts.positions.end(); f != fe; ++f) {
                if (p.first == f->first || p.first == f->second ||
                    p.second == f->first || p.second == f->second) {
                    cerr << "--position field(s) conflict with another --position" << endl;

                    exit(EXIT_FAILURE);
                }
            }
            opts.positions.push_back(p);
        }
    }
    opts.pkField = getFieldIndex(vm, opts, "primary-key");
    if (vm.count("foreign-key") > 0) {
        opts.fkField = getFieldIndex(vm, opts, "foreign-key");
        if (opts.fkField == opts.pkField) {
            cerr << "--foreign-key conflicts with --primary-key" << endl;
            exit(EXIT_FAILURE);
        }
        if (vm.count("foreign-key-index") != 1) {
            cerr << "--foreign-key-index not specified" << endl;
            exit(EXIT_FAILURE);
        }
        opts.fkIndexDir = vm["foreign-key-index"].as<string>();
    }
    opts.dupRegion = SphericalBox(vm["ra-min"].as<double>(),
                                  vm["ra-max"].as<double>(),
                                  vm["dec-min"].as<double>(),
                                  vm["dec-max"].as<double>());
    opts.numNodes = vm["num-nodes"].as<uint32_t>();
    if (opts.numNodes < 1) {
        cerr << "value of --num-nodes must be at least 1" << endl;
    }
    opts.node = vm["node"].as<uint32_t>();
    if (opts.node >= opts.numNodes) {
        cerr << "value of --node must be less than that of --num-nodes" << endl;
    }
    if (vm.count("chunk-id") > 0) {
        opts.chunkIds = vm["chunk-id"].as<vector<int32_t> >();
    }
    opts.hashChunks = vm["hash-chunks"].as<bool>();
}

} // unnamed namespace


Options::Options() :
    fields(),
    partitionPos(-1, -1),
    numThreads(1),
    blockSize(16*1024*1024),
    k(64),
    htmLevel(8),
    delimiter(','),
    inputFiles(),
    numStripes(18),
    numSubStripesPerStripe(100),
    overlap(0.1667),
    chunkIdField(-1),
    subChunkIdField(-1),
    secondarySortField(-1),
    prefix("chunk"),
    positions(),
    pkField(-1),
    fkField(-1),
    dupRegion(),
    node(0),
    numNodes(1),
    chunkIds(),
    hashChunks(false),
    indexDir("."),
    fkIndexDir(""),
    scratchDir("."),
    chunkDir(".")
{ }

Options::~Options() { }


Options const parseIndexerCommandLine(int argc, char ** argv) {
    po::options_description o;
    buildCommonOptions(o);
    buildIndexingOptions(o);
    buildCsvOptions(o);
    po::options_description output("Output options", 80);
    output.add_options()
        ("index-dir", po::value<string>()->default_value("."),
            "Output directory for index files.")
        ("scratch-dir", po::value<string>()->default_value("."),
            "Scratch directory. For maximum performance, this location "
            "should be distinct from the input file locations and output "
            "index directory at the underlying storage hardware level.");
    o.add(output);
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, o), vm);
    po::notify(vm);

    if (vm["help"].as<bool>()) {
        cout << argv[0] << " [options]\n"
"\n"
"Index one or more input CSV files in preparation for spatial data-set\n"
"duplication (via the qserv_dup utility). Three files will be written to\n"
"the output directory:\n"
"\n"
"\tdata.csv : Input CSV file, sorted by HTM ID.\n"
"\tids.bin  : 64 bit integer record IDs (primary keys),\n"
"\t           in the same sort-order as the data.csv file.\n"
"\tmap.bin  : An index into the data.csv and ids.bin files.\n"
"\t           Gives the location of CSV records and record IDs\n"
"\t           belonging to any given HTM triangle.\n"
"\n";
        cout << o << endl;
        exit(EXIT_SUCCESS);
    }
    Options opts;
    validateAndStoreCommonOptions(vm, opts);
    validateAndStoreIndexingOptions(vm, opts);
    validateAndStoreCsvOptions(vm, opts);
    opts.indexDir = vm["index-dir"].as<string>();
    opts.scratchDir = vm["scratch-dir"].as<string>();
    return opts;
}

Options const parseDuplicatorCommandLine(int argc, char ** argv) {
    po::options_description o;
    buildCommonOptions(o);
    buildPartitioningOptions(o);
    buildDuplicationOptions(o);
    po::options_description output("Output options", 80);
    output.add_options()
        ("prefix", po::value<string>()->default_value("chunk"),
            "Chunk file name prefix.")
        ("chunk-dir", po::value<string>()->default_value("."),
            "Output directory for chunk files.");
    o.add(output);
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, o), vm);
    po::notify(vm);

    if (vm["help"].as<bool>()) {
        cout << argv[0] << " [options]\n"
"\n"
"Duplicate and partition data stored in an index over an area of the sky, "
"optionally restricting output to just those chunks belonging to a single "
"node out of a group of nodes. Both position and key columns in the indexed "
"data can be remapped. To generate an index, invoke the qserv_dup_index "
"utility on a CSV export of a table."
"\n";
        cout << o << endl;
        exit(EXIT_SUCCESS);
    }
    Options opts;
    validateAndStoreCommonOptions(vm, opts);
    validateAndStorePartitioningOptions(vm, opts);
    validateAndStoreDuplicationOptions(vm, opts);
    opts.prefix = vm["prefix"].as<string>();
    opts.chunkDir = vm["chunk-dir"].as<string>();
    return opts;
}

Options const parsePartitionerCommandLine(int argc, char ** argv) {
    po::options_description o;
    buildCommonOptions(o);
    buildPartitioningOptions(o);
    buildCsvOptions(o);
    po::options_description output("Output options", 80);
    output.add_options()
        ("prefix", po::value<string>()->default_value("chunk"),
            "Chunk file name prefix.")
        ("chunk-dir", po::value<string>()->default_value("."),
            "Output directory for chunk files.");
    o.add(output);
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, o), vm);
    po::notify(vm);

    if (vm["help"].as<bool>()) {
        cout << argv[0] << " [options]\n"
"\n"
"Partition one or more input CSV files in preparation for loading into qserv.\n"
"\n";
        cout << o << endl;
        exit(EXIT_SUCCESS);
    }
    Options opts;
    validateAndStoreCommonOptions(vm, opts);
    validateAndStorePartitioningOptions(vm, opts);
    validateAndStoreCsvOptions(vm, opts);
    opts.prefix = vm["prefix"].as<string>();
    opts.chunkDir = vm["chunk-dir"].as<string>();
    return opts;
}

} // namespace dupr
