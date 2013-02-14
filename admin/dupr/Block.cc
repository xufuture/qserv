#include "Block.h"

#include <stdlib.h>
#include <algorithm>

#include "boost/scoped_array.hpp"
#include "boost/make_shared.hpp"

#include "Csv.h"
#include "Geometry.h"

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

void InputBlock::process(Options const &opts, boost::mutex & mutex, HtmIndex & idx) {
    read();
    char const * beg = _buf;
    char const * const end = beg + _sz;
    boost::scoped_array<char const *> fields(
        new char const *[opts.fields.size() + 1]());
    vector<Record> records;
    records.reserve(_sz / 1024);
    // Build a Record for every line in the input file.
    while (beg < end) {
        char const * const next = parseLine(
            beg, end, opts.delimiter, fields.get(), opts.fields.size());
        Record r;
        r.info.length = static_cast<uint32_t>(next - beg);
        r.line = beg;
        // extract ID
        if (isNull(fields[opts.pkField], fields[opts.pkField + 1] - 1)) {
            throw std::runtime_error("CSV file contains NULL primary-key value");
        }
        r.info.id = extractInt(
            fields[opts.pkField], fields[opts.pkField + 1] - 1);
        // extract ra and dec
        std::pair<double, double> sc;
        sc.first = extractDouble(fields[opts.partitionPos.first],
                                 fields[opts.partitionPos.first + 1] - 1, false);
        sc.second = extractDouble(fields[opts.partitionPos.second],
                                  fields[opts.partitionPos.second + 1] - 1, false);
        r.info.htmId = htmId(cartesian(sc), opts.htmLevel);
        records.push_back(r);
        beg = next;
    }
    // Sort input records, but not the associated lines of text.
    sort(records.begin(), records.end());
    // Extract HTM index information
    typedef vector<Record>::const_iterator RecordIter;
    vector<HtmIndex::Triangle> tris;
    RecordIter r = records.begin();
    uint32_t id = r->info.htmId;
    uint64_t numRecords = 1;
    uint64_t recordSize = r->info.length;
    ++r;
    for (RecordIter e = records.end(); r != e; ++r) {
        if (r->info.htmId != id) {
            tris.push_back(HtmIndex::Triangle(id, numRecords, recordSize));
            id = r->info.htmId;
            numRecords = 1;
            recordSize = r->info.length;
        } else {
            ++numRecords;
            recordSize += r->info.length;
        }
    }
    tris.push_back(HtmIndex::Triangle(id, numRecords, recordSize));
    typedef vector<HtmIndex::Triangle>::const_iterator TriangleIter;
    // Update the HTM index
    boost::unique_lock<boost::mutex> lock(mutex);
    for (TriangleIter t = tris.begin(), e = tris.end(); t != e; ++t) {
        idx.update(*t);
    }
    lock.unlock();
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


struct BlockWriter::Writer {
    BlockWriter * bw;

    Writer() : bw(0) { }
    Writer(BlockWriter * blockWriter) : bw(blockWriter) { }
    void operator()() {
        boost::unique_lock<boost::mutex> lock(bw->_mutex);
        bw->_started = true;
        // signal that the writer thread has started
        bw->_condition.notify_one();
        do {
            // wait for data 
            bw->_condition.wait(lock);
            if (bw->_writeSize != 0) {
                bw->_file.append(static_cast<void *>(bw->_writeBlock.get()), bw->_writeSize);
                bw->_writeSize = 0;
                // signal that the writer thread can accept another block
                bw->_condition.notify_one();
            }
        } while (!bw->_finished);
    }
};


BlockWriter::BlockWriter(std::string const & path, size_t blockSize) :
    _mutex(),
    _condition(),
    _thread(),
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
    boost::unique_lock<boost::mutex> lock(_mutex);
    _thread = boost::thread(Writer(this));
    // wait for writer thread to start
    while (!_started) {
        _condition.wait(lock);
    }
}

BlockWriter::~BlockWriter() {
    close();
}

void BlockWriter::close() {
    boost::unique_lock<boost::mutex> lock(_mutex);
    if (_finished) {
        return;
    }
    // tell writer thread to write remaining data, then exit
    if (_size > 0) {
        while (_writeSize != 0) {
            _condition.wait(lock);
        }
        swap(_writeBlock, _block);
        _writeSize = _size;
        _size = 0;
    }
    _finished = true;
    _condition.notify_one();
    lock.unlock();
    // wait for writer thread to exit
    _thread.join();
}

void BlockWriter::issue() {
    boost::unique_lock<boost::mutex> lock(_mutex);
    if (_finished) {
        throw std::runtime_error("block writer has already been closed");
    }
    assert(_size > 0);
    while (_writeSize != 0) {
        _condition.wait(lock);
    }
    swap(_writeBlock, _block);
    _writeSize = _size;
    _size = 0;
    _condition.notify_one();
}

} // namespace dupr

