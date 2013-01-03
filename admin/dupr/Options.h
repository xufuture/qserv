#ifndef OPTIONS_H
#define OPTIONS_H

#include <string>
#include <vector>

#include "Htm.h"


namespace dupr {

/// Field indexes for a pair of fields.
typedef std::pair<int, int> FieldPair;

/** Command line options for the duplicator/partitioner and indexer.
  */
struct Options {
    std::vector<std::string> fields; ///< List of field names, in order of occurence.
    FieldPair partitionPos; ///< Partitioning position field indexes.
    int numThreads;         ///< Number of threads to create.

    size_t blockSize;       ///< IO block size.
    size_t k;               ///< Merge-arity.
    int htmLevel;           ///< HTM subdivision level for population map.

    char delimiter;         ///< CSV delimiter.
    std::vector<std::string> inputFiles; ///< Input files for indexing/partitioning.

    int32_t numStripes;     ///< Number of declination stripes for the sky.
    int32_t numSubStripesPerStripe;
    double overlap;         ///< Partitioning overlap radius (deg).
    int chunkIdField;
    int subChunkIdField;
    int secondarySortField;
    std::string prefix;     ///< Chunk file name prefix.

    std::vector<FieldPair> positions; ///< Positions to be remapped by the duplicator.
    int pkField;            ///< Primary key field to be remapped by the duplicator
                            ///  (e.g. sourceId in Source).
    int fkField;            ///< Foreign key field to be remapped 
                            ///  (e.g. objectId in Source) or -1.
    SphericalBox dupRegion; ///< Region the duplicator should generate data for.
    uint32_t node;          ///< Node to generate chunks for.
    uint32_t numNodes;      ///< Total number of nodes.
    std::vector<int32_t> chunkIds; ///< Chunk IDs to generate data for.
    bool hashChunks;        ///< Hash chunks to nodes?

    std::string indexDir;   ///< Input/output index directory.
    std::string fkIndexDir; ///< Foreign key index directory.
    std::string scratchDir; ///< Scratch directory for external merge sort.
    std::string chunkDir;   ///< Output directory for chunks.

    Options();
    ~Options();
};


Options const parseIndexerCommandLine(int argc, char ** argv);
Options const parseDuplicatorCommandLine(int argc, char ** argv);
Options const parsePartitionerCommandLine(int argc, char ** argv);

} // namespace dupr

#endif
