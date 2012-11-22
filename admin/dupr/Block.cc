#include "Block.h"

#include <stdlib.h>
#include <algorithm>

#include "boost/scoped_array.hpp"
#include "boost/make_shared.hpp"

#include "Csv.h"
#include "Htm.h"

using std::string;
using std::sort;
using std::swap;
using std::vector;

using boost::shared_ptr;
using boost::make_shared;


namespace dupr {

InputBlock::~InputBlock() {
    free(_buf);
}

void InputBlock::read() {
    if (!_buf) {
        _buf = static_cast<char *>(_f->read(0, _off, _sz));
    }
}

void InputBlock::process(Options const &opt, PopulationMap & map) {
    read();
    char * beg = _buf;
    char * const end = beg + _sz;
    boost::scoped_array<char *> fields(new char *[opt.numFields + 1]());
    vector<Record> records;
    records.reserve(_sz / 1024);
    // Build a Record for every line in the input file.
    while (beg < end) {
        char * const next = parseLine(
            beg, end, opt.delimiter, fields.get(), opt.numFields);
        Record r;
        r.info.length = static_cast<uint32_t>(next - beg);
        r.line = beg;
        // extract ID
        r.info.id = extractInt(
            fields[opt.pkField], fields[opt.pkField + 1] - 1);
        // extract ra and dec
        Eigen::Vector2d sc;
        sc(0) = extractDouble(fields[opt.partitionPos.raField],
                              fields[opt.partitionPos.raField + 1] - 1);
        sc(1) = extractDouble(fields[opt.partitionPos.decField],
                              fields[opt.partitionPos.decField + 1] - 1);
        r.info.htmId = htmId(cartesian(sc), opt.htmLevel);
        records.push_back(r);
        beg = next;
    }
    // Sort input records, but not the associated lines of text.
    // TODO: would sorting lines here make merging faster later, i.e.
    // due to better cache behavior?
    sort(records.begin(), records.end());
    // Update population map. This could be done at the end (during the final
    // merge pass that produces the sorted data file), but doing it here
    // makes the merge implementation simpler.
    typedef vector<Record>::const_iterator RecordIter;
    RecordIter i = records.begin();
    uint32_t htmId = i->info.htmId;
    uint64_t nrec = 1;
    uint64_t sz = i->info.length;
    ++i;
    for (RecordIter e = records.end(); i != e; ++i) {
        if (i->info.htmId != htmId) {
            map.add(htmId, nrec, sz);
            htmId = i->info.htmId;
            nrec = 1;
            sz = i->info.length;
        } else {
            ++nrec;
            sz += i->info.length;
        }
    }
    map.add(htmId, nrec, sz);
    // save sorted records
    swap(_recs, records);
}


InputBlockVector const splitInputs(vector<string> const & paths, size_t blockSize) {
    char buf[MAX_LINE_SIZE];
    InputBlockVector blocks;
    // sanity checks
    if (blockSize < 2*1024*1024 || blockSize < 2*MAX_LINE_SIZE) {
        throw std::runtime_error("Input block size must be >= 2MiB");
    } else if (blockSize > 1024*1024*1024) {
        throw std::runtime_error("Input block size must be <= 1GiB");
    }
    typedef vector<string>::const_iterator StringIter;
    for (StringIter i = paths.begin(), e = paths.end(); i != e; ++i) {
        // open input file
        shared_ptr<InputFile> f = make_shared<InputFile>(*i);
        // initially block the file into pieces [0, B), [B, 2*B), ...
        // then walk backwards from each boundary to find a line terminator,
        // thus guaranteeing that no line spans a block.
        for (off_t start = 0, i = 1; start < f->size(); ++i) {
            off_t end = i * static_cast<off_t>(blockSize);
            if (end >= f->size()) {
                blocks.push_back(make_shared<InputBlock>(
                    f, start, static_cast<size_t>(f->size() - start)));
                start = end;
            } else {
                f->read(buf, end - MAX_LINE_SIZE, MAX_LINE_SIZE);
                int i = MAX_LINE_SIZE - 1;
                while (i >= 0 && buf[i] != '\n') { --i; }
                if (i < 0) {
                    throw std::runtime_error("line too long");
                }
                end = end - (MAX_LINE_SIZE - (1 + i));
                blocks.push_back(make_shared<InputBlock>(
                    f, start, static_cast<size_t>(end - start)));
                start = end;
            }
        }
    }
    return blocks;
}


BlockWriter::BlockWriter(std::string const & path, size_t blockSize) :
    _mutex(),
    _condition(),
    _file(path),
    _size(0),
    _blockSize(blockSize),
    _block(new char[blockSize]),
    _off(0),
    _writeSize(0),
    _writeBlock(new char[blockSize]),
    _started(false),
    _finished(false)
{
    if (_blockSize == 0) {
        throw std::runtime_error("zero is not a legal block size");
    }
    Lock lock(_mutex);
    // create writer thread
    if (::pthread_create(&_thread, 0, &run, static_cast<void *>(this)) != 0) {
        throw std::runtime_error("pthread_create() failed");
    }
    // wait for writer thread to start
    while (!_started) {
        _condition.wait(lock);
    }
}

BlockWriter::~BlockWriter() {
    close();
}

void BlockWriter::close() {
    Lock lock(_mutex);
    assert(_writeSize == 0);
    if (_finished) {
        return;
    }
    // tell writer thread to write remaining data, then exit
    _finished = true;
    if (_size > 0) {
        swap(_writeBlock, _block);
        _writeSize = _size;
        _size = 0;
    }
    _condition.notify();
    lock.release();
    // wait for writer thread to exit
    ::pthread_join(_thread, 0);
}

void BlockWriter::issue() {
    Lock lock(_mutex);
    if (_finished) {
        throw std::runtime_error("block writer has already been closed");
    }
    assert(_size > 0);
    assert(_writeSize == 0);
    // swap buffers
    swap(_writeBlock, _block);
    _writeSize = _size;
    _size = 0;
    _condition.notify();
}

void * BlockWriter::run(void * arg) {
    BlockWriter * w = reinterpret_cast<BlockWriter *>(arg);
    Lock lock(w->_mutex);
    w->_started = true;
    // signal that the writer thread has started
    w->_condition.notify();
    do {
        // wait for data
        w->_condition.wait(lock);
        if (w->_writeSize != 0) {
            w->_file.append(static_cast<void *>(w->_writeBlock.get()), w->_writeSize);
            w->_writeSize = 0;
        }
    } while (!w->_finished);
    return 0;
}

} // namespace dupr

