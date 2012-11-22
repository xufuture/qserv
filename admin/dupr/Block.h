#ifndef BLOCK_H
#define BLOCK_H

#include <pthread.h>
#include <vector>

#include "boost/scoped_array.hpp"
#include "boost/shared_ptr.hpp"

#include "FileUtils.h"
#include "Htm.h"
#include "Options.h"
#include "ThreadUtils.h"


namespace dupr {

/** Information extracted/derived from an input record.
  */
struct RecordInfo {
    uint32_t htmId;  // HTM ID of record.
    uint32_t length; // Line length.
    int64_t id;      // Integer record ID.

    bool operator<(RecordInfo const r) const {
        return htmId < r.htmId;
    }
};

/** An input record.
  */
struct Record {
    RecordInfo info;
    char const * line;

    bool operator<(Record const r) const {
        return info < r.info;
    }
};


/** A piece of an input file.
  */
class InputBlock {
public:
    InputBlock(boost::shared_ptr<InputFile> const & f, off_t off, size_t sz) :
        _f(f), _off(off), _sz(sz), _buf(0), _recs() { }

    ~InputBlock();

    /// Read block contents.
    void read();

    /// Process block contents.
    void process(Options const & opt, PopulationMap & map);

    /// Return block records; empty until process() has been called.
    std::vector<Record> const & records() const { return _recs; }

private:
    // Disable copy-construction and assignment.
    InputBlock(InputBlock const &);
    InputBlock & operator=(InputBlock const &);

    boost::shared_ptr<InputFile> _f; // input file
    off_t _off;  // starting offset in file (bytes)
    size_t _sz;  // size of block (bytes)
    char * _buf; // buffer containing _sz bytes
    std::vector<Record> _recs;
};

typedef std::vector<boost::shared_ptr<InputBlock> > InputBlockVector;


/** Break input text files into a series of blocks containing approximately
    blockSize bytes. Block boundaries are chosen such that each line is
    contained in exactly one block. Note that all input files are opened,
    so a smaller number of large input files is preferred over a larger
    number of small files - the latter can cause the per-process file
    descriptor limit to be reached.

    For simplicity, the distribution of input files over devices is
    currently not taken into account.
  */
InputBlockVector const splitInputs(std::vector<std::string> const & paths,
                                   size_t blockSize);


/** Asynchronous block writer.

    Allocates memory for two blocks and starts a writer thread. Data can be
    appended to one block while the other block is written to disk.

    Note that a block writer should be used by a single thread at a time.
  */
class BlockWriter {
public:
    BlockWriter(std::string const & path, size_t blockSize);
    ~BlockWriter();

    /// Return the path of the output file.
    std::string const & path() const { return _file.path(); }

    /// Write out sz bytes of data.
    inline void append(void const * data, size_t sz) {
        size_t i = _size;
        size_t n = _blockSize - i;
        if (sz >= n) {
            memcpy(_block.get() + i, data, n);
            issue();
            if (sz > n) {
                memcpy(_block.get(), static_cast<char const *>(data) + n, sz - n);
                _size = sz - n;
            }
        } else {
            memcpy(_block.get() + i, data, sz);
            _size = i + sz;
        }
        _off += sz;
    }

    /// Return the total number of bytes written so far.
    off_t tell() const { return _off; }

    /// Flush and close the writer. Any further call to append() is an error.
    void close();

private:
    // Disable copy-construction and assignment.
    BlockWriter(BlockWriter const &);
    BlockWriter & operator=(BlockWriter const &);

    static void * run(void * arg);
    void issue();

    char                      _cl0[CACHE_LINE_SIZE];

    Mutex                     _mutex;
    Condition                 _condition;
    ::pthread_t               _thread;
    OutputFile                _file;
    size_t                    _size;
    size_t                    _blockSize;
    boost::scoped_array<char> _block;
    off_t                     _off;
    size_t                    _writeSize;
    boost::scoped_array<char> _writeBlock;
    bool                      _started;
    bool                      _finished;

    char                      _cl1[CACHE_LINE_SIZE];
};

}  // namespace dupr

#endif
