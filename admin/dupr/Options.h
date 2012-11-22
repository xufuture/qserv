#ifndef OPTIONS_H
#define OPTIONS_H

#include <string>
#include <vector>


namespace dupr {

/** Index for right ascension and declination fields.
  */
struct Position {
    int raField;
    int decField;

    Position() : raField(-1), decField(-1) { }
};


/** Command line options for the duplicator/partitioner and indexer.
  */
struct Options {
    size_t                   blockSize;    // IO block size.
    size_t                   k;            // Number of input blocks to merge at once.

    int                      numThreads;   // Number of threads to create.

    int                      htmLevel;     // HTM subdivision level for input population map.
    int                      numFields;    // Number of fields per record.
    char                     delimiter;    // CSV delimiter.

    int                      pkField;      // Index of primary key field (e.g. sourceId in Source).
    Position                 partitionPos; // Partitioning position field indexes. 

    std::vector<std::string> inputFiles;   // Input files for indexer.
    std::string              indexDir;     // Directory containing input/output index files.
    std::string              scratchDir;   // Scratch directory for merger.
};


/// Parse indexer command line.
Options const indexerCommandLine(int argc, char ** argv);

} // namespace dupr

#endif
