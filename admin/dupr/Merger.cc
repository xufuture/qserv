#include "Merger.h"

#include <sys/mman.h>
#include <algorithm>
#include <stdexcept>


using std::make_heap;
using std::pop_heap;
using std::push_heap;
using std::string;
using std::vector;

using boost::shared_ptr;


namespace dupr {

namespace {

struct InputRun {
    vector<Record>::const_iterator beg;
    vector<Record>::const_iterator end;

    void initialize() { }
    
    bool advance() {
        return ++beg == end;
    }
    
    bool operator<(InputRun const & r) const {
        return *r.beg < *beg;
    }

    Record const & get() const {
        return *beg;
    }
};

size_t const MERGE_BLOCK_SIZE = 2*1024*1024;

struct ScratchRunData {
    Record rec;
    char const * beg;
    char const * end;

    void initialize() {
        size_t addr = reinterpret_cast<size_t>(beg);
        size_t paddr = (addr & ~(MERGE_BLOCK_SIZE - 1)) + MERGE_BLOCK_SIZE;
        addr -= addr % static_cast<size_t>(sysconf(_SC_PAGESIZE));
        // request residency for initial data in merge run
        madvise(reinterpret_cast<void *>(addr),
                MERGE_BLOCK_SIZE + paddr - addr, MADV_WILLNEED);
        memcpy(&rec.info, beg, sizeof(RecordInfo));
        rec.line = beg + sizeof(RecordInfo);
        beg = reinterpret_cast<char *>(paddr);
    }

    bool advance() {
        char const * next = rec.line + rec.info.length;
        if (next == end) {
            return true;
        }
        memcpy(&rec.info, next, sizeof(RecordInfo));
        next += sizeof(RecordInfo);
        rec.line = next;
        if (next >= beg) {
            // free behind
            madvise(const_cast<char *>(beg - MERGE_BLOCK_SIZE),
                    MERGE_BLOCK_SIZE, MADV_DONTNEED);
            beg += MERGE_BLOCK_SIZE;
            if (beg + MERGE_BLOCK_SIZE < end) {
                // prefetch next merge block
                madvise(const_cast<char *>(beg + MERGE_BLOCK_SIZE),
                        MERGE_BLOCK_SIZE, MADV_WILLNEED);
            }
        }
        return false;
    }
};

struct ScratchRun {
    ScratchRunData * data;

    void initialize() {
        data->initialize();
    }

    bool advance() {
        return data->advance();
    }

    bool operator<(ScratchRun const & s) const {
        return s.data->rec < data->rec;
    }

    Record const & get() const {
        return data->rec;
    }
};

} // unnamed namespace


Merger::Merger(
    string const & dataFile,
    string const & idFile,
    string const & scratchFile,
    size_t blockSize,
    size_t k,
    size_t numInputBlocks
) :
    _mutex(),
    _fullCondition(),
    _mergeCondition(),
    _inputBlocks(),
    _k(k),
    _numInputBlocks(numInputBlocks),
    _merging(false),
    _scratchWriter(),
    _dataWriter(new BlockWriter(dataFile, blockSize)),
    _idWriter(new BlockWriter(idFile, blockSize))
{
    if (k < 2) {
        throw std::runtime_error("merge factor k must be >= 2");
    }
    if (numInputBlocks == 0) {
        throw std::runtime_error("no input blocks");
    }
    if (numInputBlocks > k) {
        _scratchBlocks.reserve(numInputBlocks/k + 1);
        _scratchWriter.reset(new BlockWriter(scratchFile, blockSize));
    }
}

Merger::~Merger() {
    _dataWriter->close();
    _idWriter->close();
}

void Merger::add(shared_ptr<InputBlock> const & block) {
    InputBlockVector blocks;
    {
        Lock lock(_mutex);
        assert(_numInputBlocks > 0);
        // wait until there is space in the queue
        while (_inputBlocks.size() == _k) {
            _fullCondition.wait(lock);
        }
        assert(_numInputBlocks > 0);
        --_numInputBlocks;
        _inputBlocks.push_back(block);
        if (_inputBlocks.size() < _k && _numInputBlocks != 0) {
            return;
        }
        // got the last input block, or k input blocks are available.
        while (_merging) {
            // wait for any in-progress merge to finish.
            _mergeCondition.wait(lock);
        }
        _merging = true; // become the merge thread
        assert(_inputBlocks.size() == _k ||
               (_numInputBlocks == 0 && !_inputBlocks.empty()));
        // grab input blocks
        blocks.reserve(_k);
        swap(blocks, _inputBlocks);
        // unblock threads waiting to add input blocks to the queue
        _fullCondition.notifyAll();
    }
    // perform the merge
    std::vector<InputRun> runs;
    runs.reserve(_k);
    typedef InputBlockVector::const_iterator BlockIter;
    for (BlockIter i = blocks.begin(), e = blocks.end(); i != e; ++i) {
        InputRun r;
        r.beg = (*i)->records().begin();
        r.end = (*i)->records().end();
        runs.push_back(r);
    }
    if (_scratchWriter) {
        ScratchBlock b;
        b.beg = _scratchWriter->tell();
        merge<InputRun, false>(runs);
        b.end = _scratchWriter->tell();
        _scratchBlocks.push_back(b);
    } else {
        // this is the final pass
        merge<InputRun, true>(runs);
    }
    // unblock thread waiting to merge the next sequence of input blocks
    Lock lock(_mutex);
    _merging = false;
    _mergeCondition.notify();
}

void Merger::finish() {
    if (!_scratchWriter) {
        return;
    }
    // close the scratch file
    string path = _scratchWriter->path();
    _scratchWriter->close();
    _scratchWriter.reset(0);
    vector<ScratchRunData> data;
    vector<ScratchRun> runs;
    data.reserve(_scratchBlocks.size());
    runs.reserve(_scratchBlocks.size());
    // map the entire scratch file into memory
    MappedInputFile f(path);
    // build scratch runs
    typedef vector<ScratchBlock>::const_iterator BlockIter;
    for (BlockIter i = _scratchBlocks.begin(), e = _scratchBlocks.end(); i != e; ++i) {
        ScratchRunData d;
        d.beg = f.data() + i->beg;
        d.end = f.data() + i->end;
        data.push_back(d);
        ScratchRun r;
        r.data = &data.back();
        runs.push_back(r);
    }
    // merge scratch runs to the output files
    merge<ScratchRun, true>(runs);
    // TODO: this is a cop out - mmap isn't the best way to go about doing IO,
    // and scratch blocks are handled in one pass, no matter how many there are.
    // But for now, go with what's easy to implement.
}

template <typename Run, bool finalPass>
void Merger::merge(vector<Run> & runs) {
    typedef typename vector<Run>::iterator RunIter;
    for (RunIter i = runs.begin(), e = runs.end(); i != e; ++i) {
        i->initialize();
    }
    make_heap(runs.begin(), runs.end());
    while (!runs.empty()) {
        pop_heap(runs.begin(), runs.end());
        Record const & rec = runs.back().get();
        // write out the next record
        if (finalPass) {
            // write to destination files
            _dataWriter->append(rec.line, rec.info.length);
            _idWriter->append(&rec.info.id, sizeof(rec.info.id));
        } else {
            // write to scratch file
            _scratchWriter->append(&rec.info, sizeof(rec.info));
            _scratchWriter->append(rec.line, rec.info.length);
        }
        if (runs.back().advance()) {
            runs.pop_back();
        } else {
            push_heap(runs.begin(), runs.end());
        }
    }
}

} // namespace dupr

