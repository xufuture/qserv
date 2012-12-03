#include "Options.h"

#include <iostream>

#include "boost/program_options.hpp"

#include "Htm.h"

using std::cerr;
using std::cout;
using std::endl;
using std::find;
using std::string;
using std::vector;

namespace po = boost::program_options;


namespace dupr {

namespace {

void buildCommonOptions(po::options_description & options) {
    po::options_description general("General options", 80);
    general.add_options()
        ("help,h", po::bool_switch(),
            "Print usage help.")
        ("num-threads,j", po::value<int>()->default_value(4),
            "Number of threads to launch.")
        ("index-dir,i", po::value<string>()->default_value("."),
            "Index file directory. Written by the indexer and read from "
            "by the duplicator. Must be specified exactly once.");
    po::options_description input("Input options", 80);
    input.add_options()
        ("delimiter,d", po::value<char>()->default_value(','),
            "CSV delimiter character.")
        ("fields,f", po::value<string>(),
            "Comma separated list of field names in input file(s) or index.")
        ("partitioned-by,p", po::value<string>(),
            "Name of partitioning right ascension and declination "
            "fields, separated by a comma. Must be specified exactly once.")
        ("primary-key,k", po::value<string>(),
            "Name of primary record ID field, e.g. sourceId in the "
            "Source table. Must be specified exactly once.");
    options.add(general).add(input);
}

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

} // unnamed namespace

void buildIndexerOptions(po::options_description & options) {
    buildCommonOptions(options);
    po::options_description indexer("Indexer options", 80);
    indexer.add_options()
        ("merge-arity,K", po::value<size_t>()->default_value(32),
            "Number of input blocks to merge at a time.")
        ("block-size,B", po::value<size_t>()->default_value(32),
            "IO block size in MiB")
        ("htm-level,L", po::value<int>()->default_value(8),
            "HTM subdivision level.")
        ("input,I", po::value<vector<string> >(),
            "Input CSV file. Must be specified at least once.")
        ("scratch-dir,S", po::value<string>()->default_value("."),
            "Scratch directory. For maximum performance, this location "
            "should be distinct from the input file locations and output "
            "index directory at the underlying storage hardware level.");
    options.add(indexer);
}

void buildDuplicatorOptions(po::options_description & options) {
    buildCommonOptions(options);
    po::options_description duplicator("Duplicator options", 80);
    duplicator.add_options()
        ("position,P", po::value<vector<string> >(),
            "Name of right ascension and declination fields, separated by a "
            "comma. The duplicator will remap these along with the partitioning "
            "position. May be specified any number of times.")
        ("foreign-key,F", po::value<vector<string> >(),
            "Name of a foreign record ID field (e.g. objectId in the Source "
            "table) and the corresponding index directory, separated by a "
            "comma. May be specified any number of times.");
    // TODO: these are incomplete
    options.add(duplicator);
}

Options const indexerCommandLine(int argc, char ** argv) {
    po::options_description o;
    buildIndexerOptions(o);
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, o), vm);
    po::notify(vm);

    if (vm["help"].as<bool>()) {
        cout << argv[0] << " [options]\n"
"\n"
"Indexes one or more input CSV files (specified via --input) in preparation\n"
"for spatial data-set duplication. Three files will be written to the output\n"
"directory:\n"
"\n"
"\tdata.csv : Input CSV file, sorted by HTM ID.\n"
"\tids.bin  : 64 bit integer record IDs (primary keys),\n"
"\t           in the same sort-order as the data file.\n"
"\tmap.bin  : An index into the data and ids files. Gives\n"
"\t           the location of CSV records and record IDs\n"
"\t           belonging to any given HTM triangle.\n"
"\n";
        cout << o << endl;
        exit(EXIT_SUCCESS);
    }
    Options opts;
    opts.blockSize = vm["block-size"].as<size_t>()*1024*1024;
    if (opts.blockSize < 2*1024*1024) {
        cerr << "--block-size must be at least 2 (MiB)" << endl;
        exit(EXIT_FAILURE);
    }
    opts.k = vm["merge-arity"].as<size_t>();
    if (opts.k < 2) {
        cerr << "--merge-arity must be at least 2" << endl;
        exit(EXIT_FAILURE);
    }
    opts.numThreads = vm["num-threads"].as<int>();
    if (opts.numThreads < 1 || opts.numThreads > 100) {
        cerr << "--num-threads value must be between 1 and 100" << endl;
        exit(EXIT_FAILURE);
    }
    opts.htmLevel = vm["htm-level"].as<int>();
    if (opts.htmLevel < 0 || opts.htmLevel > HTM_MAX_LEVEL) {
        cerr << "--htm-level value must be an integer between 0 and "
             << HTM_MAX_LEVEL << endl;
        exit(EXIT_FAILURE);
    }
    opts.delimiter = vm["delimiter"].as<char>();
    if (opts.delimiter == '\\' || opts.delimiter == '"' || opts.delimiter == '\n') {
        cerr << "--delimiter cannot be '\\' '\\n' or '\"'"<< endl;
        exit(EXIT_FAILURE);
    }
    if (vm.count("input") == 0) {
        cerr << "No inputs specified" << endl;
        exit(EXIT_FAILURE);
    }
    opts.inputFiles = vm["input"].as<vector<string> >();
    opts.indexDir = vm["index-dir"].as<string>();
    opts.scratchDir = vm["scratch-dir"].as<string>();
    if (vm.count("fields") != 1) {
        cerr << "--fields not specified"<< endl;
        exit(EXIT_FAILURE);
    }
    vector<string> fields = split(vm["fields"].as<string>());
    opts.numFields = static_cast<int>(fields.size());
    if (vm.count("partitioned-by") != 1) {
        cerr << "--partitioned-by not specified"<< endl;
        exit(EXIT_FAILURE);
    }
    vector<string> p = split(vm["partitioned-by"].as<string>());
    if (p.size() != 2 || p[0].empty() || p[1].empty() || p[0] == p[1]) {
        cerr << "--partitioned-by must consist of two distinct and "
                "non-empty field names, separated by a comma"<< endl;
        exit(EXIT_FAILURE);
    }
    typedef vector<string>::const_iterator StringIter;
    StringIter i = find(fields.begin(), fields.end(), p[0]);
    if (i == fields.end()) {
        cerr << p[0] + " field specified via --partitioned-by "
                "is not in the schema" << endl;
        exit(EXIT_FAILURE);
    }
    opts.partitionPos.raField = static_cast<int>(i - fields.begin());
    i = find(fields.begin(), fields.end(), p[1]);
    if (i == fields.end()) {
        cerr << p[1] + " field specified via --partitioned-by "
                "is not in the schema" << endl;
        exit(EXIT_FAILURE);
    }
    opts.partitionPos.decField = static_cast<int>(i - fields.begin());
    if (vm.count("primary-key") != 1) {
        cerr << "--primary-key not specified"<< endl;
        exit(EXIT_FAILURE);
    }
    string pk = vm["primary-key"].as<string>();
    i = find(fields.begin(), fields.end(), pk);
    if (i == fields.end()) {
        cerr << pk + " field specified via --primary-key "
                "is not in the schema" << endl;
        exit(EXIT_FAILURE);
    }
    opts.pkField = static_cast<int>(i - fields.begin());
    return opts;
}

} // namespace dupr
