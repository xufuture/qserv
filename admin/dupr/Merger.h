#ifndef MERGER_H
#define MERGER_H

#include <string>
#include <vector>

#include "boost/scoped_ptr.hpp"
#include "boost/thread.hpp"

#include "Block.h"


namespace dupr {

class Merger {
public:
    Merger(std::string const & dataFile,
           std::string const & idFile,
           std::string const & scratchFile,
           size_t blockSize,
           size_t k,
           size_t numInputBlocks);

    ~Merger();

    /// Add a processed block to the merge queue. May be called from
    /// multiple threads.
    void add(boost::shared_ptr<InputBlock> const & block);

    /// Produce the sorted output data and ID files.
    void finish();

private:

    struct ScratchBlock {
        off_t beg;
        off_t end;
    };

    template <typename Run, bool finalPass>
    void merge(std::vector<Run> & runs);

    char                           _cl0[CACHE_LINE_SIZE];

    boost::mutex                   _mutex;
    boost::condition_variable      _fullCondition;
    boost::condition_variable      _mergeCondition;
    InputBlockVector               _inputBlocks;
    size_t                         _k;
    size_t                         _numInputBlocks;
    bool                           _merging;

    char                           _cl1[CACHE_LINE_SIZE];

    std::vector<ScratchBlock>      _scratchBlocks;
    boost::scoped_ptr<BlockWriter> _scratchWriter;
    boost::scoped_ptr<BlockWriter> _dataWriter;
    boost::scoped_ptr<BlockWriter> _idWriter;

    char                           _cl2[CACHE_LINE_SIZE];
};

} // namespace dupr

#endif // MERGER_H
