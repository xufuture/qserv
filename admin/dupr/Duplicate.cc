#include <sys/types.h>
#include <sys/mman.h>
#include <stdint.h>
#include <stdio.h>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "boost/make_shared.hpp"
#include "boost/scoped_array.hpp"
#include "boost/scoped_ptr.hpp"
#include "boost/timer.hpp"

#include "Block.h"
#include "Csv.h"
#include "Htm.h"
#include "Options.h"
#include "ThreadUtils.h"


using std::cout;
using std::endl;
using std::flush;
using std::max;
using std::string;
using std::vector;

using boost::make_shared;
using boost::scoped_ptr;
using boost::scoped_array;
using boost::shared_ptr;

using Eigen::Vector2d;
using Eigen::Matrix3d;

using namespace dupr;


namespace {

struct KeyInfo {
    shared_ptr<PopulationMap const> map;     // Population map.
    shared_ptr<MappedInputFile const> file;  // File containing ID/key values.
    int fieldIndex;                          // ID/key field index.

    KeyInfo() : map(), file(), fieldIndex(-1) { }
};


/** The key K of a record in HTM trixel with ID H is mapped to K' in
    HTM trixel with ID H' as follows:

    K' = H' * 2^32 + s(K)

    where the function s(K) gives the number of keys in H with values
    less than K. Because the duplication index stores records sorted
    by (HTM ID, key), this is simply the index of the record with key
    K in H, and can be found via binary search.
  */
class KeyMapper {
public:
    KeyMapper() : _beg(0), _end(0), _htmId(0), _fieldIndex(-1) { }

    KeyMapper(KeyInfo const & key,
              uint32_t sourceHtmId,
              uint32_t destinationHtmId);

    int getFieldIndex() const {
        return _fieldIndex;
    }

    int64_t map(int64_t key) const {
        int64_t const * p = std::lower_bound(_beg, _end, key);
        assert(p != _end && key == *p);
        return (static_cast<int64_t>(_htmId) << 32) + (p - _beg);
    }

private:
    int64_t const * _beg;
    int64_t const * _end;
    uint32_t _htmId;
    int _fieldIndex;
};

KeyMapper::KeyMapper(
    KeyInfo const & key,
    uint32_t sourceHtmId,
    uint32_t destinationHtmId
) :
    _htmId(destinationHtmId),
    _fieldIndex(key.fieldIndex)
{
    size_t off = sizeof(int64_t) * static_cast<size_t>(
        key.map->getNumRecordsBelow(sourceHtmId));
    size_t sz = static_cast<size_t>(key.map->getNumRecords(sourceHtmId));
    _beg = reinterpret_cast<int64_t const *>(key.file->data() + off);
    _end = _beg + sz;
    size_t pageSize = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    size_t beg = off & ~(pageSize - 1);
    size_t end = (off + sz*sizeof(int64_t) + pageSize - 1) & ~(pageSize - 1);
    char * madvStart = const_cast<char *>(key.file->data()) + beg;
    size_t madvSize = end - beg;
    // prefetch IDs into memory
    madvise(madvStart, madvSize, MADV_WILLNEED);
}


class PosMapper {
public:
    PosMapper() : _m(Matrix3d::Identity()) { }

    PosMapper(uint32_t sourceHtmId,
              uint32_t destinationHtmId);

    Vector2d const map(Vector2d const & pos) const {
        return spherical(_m * cartesian(pos));
    }

private:
    Matrix3d _m;
};

PosMapper::PosMapper(uint32_t sourceHtmId, uint32_t destinationHtmId) : _m() {
    Trixel src(sourceHtmId);
    Trixel dst(destinationHtmId);
    _m = dst.getCartesianTransform() * src.getBarycentricTransform();
}


/** Output chunk record.
  */
struct ChunkRecord {
    ChunkLocation loc;
    uint32_t length;
    int64_t sortKey;
    char const * line;

    bool operator<(ChunkRecord const r) const {
        return loc.subChunkId < r.loc.subChunkId ||
               (loc.subChunkId == r.loc.subChunkId && sortKey < r.sortKey);
    }
};

/** A sorted run of output records.
  */
struct ChunkRecordRun {
    vector<ChunkRecord>::const_iterator beg;
    vector<ChunkRecord>::const_iterator end;

    bool advance() {
        return ++beg == end;
    }
    
    bool operator<(ChunkRecordRun const & r) const {
        return *r.beg < *beg;
    }

    ChunkRecord const & get() const {
        return *beg;
    }
};


/** A list of output chunk records, along with storage for the
    associated CSV data.
  */
class OutputBlock {
public:
    OutputBlock();
    ~OutputBlock();

    void add(ChunkRecord const & r);

    vector<ChunkRecord> const & getRecords() const {
        return _recs;
    }

private:
    static size_t const LINE_BLOCK_SIZE = 4*1024*1024;

    vector<ChunkRecord> _recs;
    vector<char *> _lineBlocks;
    char * _line;
    char * _end;
};

typedef vector<shared_ptr<OutputBlock> > OutputBlockVector;

OutputBlock::OutputBlock() : _recs(), _lineBlocks(), _line(0), _end(0) {
    _recs.reserve(8192);
    _lineBlocks.reserve(16);
}

OutputBlock::~OutputBlock() {
    typedef vector<char *>::const_iterator LbIter;
    for (LbIter i = _lineBlocks.begin(), e = _lineBlocks.end(); i != e; ++i) {
        free(*i);
    }
}

void OutputBlock::add(ChunkRecord const & r) {
    if (static_cast<uint32_t>(_end - _line) < r.length) {
        // allocate another line block
        _line = static_cast<char *>(malloc(LINE_BLOCK_SIZE));
        if (_line == 0) {
            throw std::runtime_error("malloc() failed");
        }
        _lineBlocks.push_back(_line);
        _end = _line + LINE_BLOCK_SIZE;
    }
    memcpy(_line, r.line, r.length);
    _recs.push_back(r);
    _recs.back().line = _line;
    _line += r.length;
}


/** An ASCII representation of an integral or floating point field value.
  */
struct FieldValue {
    unsigned char length;
    char buf[31];

    FieldValue() : length(0) { }

    void set(int64_t val) {
        int n = snprintf(buf, sizeof(buf), "%lld", static_cast<long long>(val));
        assert(n > 0 && n < static_cast<int>(sizeof(buf) - 1));
        length = static_cast<unsigned char>(n);
    }

    void set(double val) {
        int n = snprintf(buf, sizeof(buf), "%.17g", val);
        assert(n > 0 && n < static_cast<int>(sizeof(buf) - 1));
        length = static_cast<unsigned char>(n);
    }

    void clear() {
        length = 0;
    }
};


class ChunkDuplicator;

/** Duplicator that handles a single trixel at a time.

    Multiple threads construct a TrixelDuplicator from the same
    ChunkDuplicator, then retrieve HTM IDs to generate data for until
    the ChunkDuplicator has no more HTM IDs left in its queue.
  */
class TrixelDuplicator {
public:
    TrixelDuplicator(ChunkDuplicator const & dup);

    void duplicate();

private:
    TrixelDuplicator(TrixelDuplicator const &);
    TrixelDuplicator & operator=(TrixelDuplicator const &);

    void setupTrixel(uint32_t htmId);
    void processTrixel();
    void finishTrixel();
    uint32_t buildOutputLine();

    ChunkDuplicator const &    _dup;
    Options const &            _opts;
    char const *               _inputLine;
    char const *               _inputEnd;
    bool                       _mapPositions;
    PosMapper                  _posMapper;
    scoped_array<KeyMapper>    _keyMappers;
    int                        _chunkIdField;
    int                        _subChunkIdField;
    size_t                     _numOutputFields;
    scoped_array<char const *> _fields;
    scoped_array<FieldValue>   _values;
    shared_ptr<OutputBlock>    _block;
    vector<ChunkLocation>      _locations;
    char                       _outputLine[MAX_LINE_SIZE];
    char *                     _madvStart;
    size_t                     _madvSize;
};


/** Duplicator that handles a single chunk at a time.
  */
class ChunkDuplicator {
public:
    ChunkDuplicator(Options const & options);

    void duplicate();

private:
    ChunkDuplicator(ChunkDuplicator const &);
    ChunkDuplicator & operator=(ChunkDuplicator const &);

    void generateChunk();
    void finishChunk();

    static void * run(void * arg);

    Options const &   _opts;
    Chunker           _chunker;
    KeyInfo           _primary;
    vector<KeyInfo>   _foreignKeys;
    MappedInputFile   _dataFile;
    vector<int32_t>   _chunkIds;
    int32_t           _chunkId;

    char                      _cl0[CACHE_LINE_SIZE];

    mutable Mutex             _mutex;
    mutable vector<uint32_t>  _htmIds;
    mutable OutputBlockVector _blocks;

    char                      _cl1[CACHE_LINE_SIZE];

    friend class TrixelDuplicator;
};

ChunkDuplicator::ChunkDuplicator(Options const & opts) :
    _opts(opts),
    _chunker(opts.overlap, opts.numStripes, opts.numSubStripesPerStripe),
    _primary(),
    _foreignKeys(),
    _dataFile(opts.indexDir + "/data.csv"),
    _chunkIds(),
    _chunkId(-1),
    _mutex(),
    _htmIds(),
    _blocks()
{
    // Read in population maps, and memory-map required CSV/id files
    _primary.map = make_shared<PopulationMap>(opts.indexDir + "/map.bin");
    _primary.file = make_shared<MappedInputFile>(opts.indexDir + "/ids.bin");
    _primary.fieldIndex = opts.pkField;
    typedef vector<FieldAndIndex>::const_iterator FiIter;
    for (FiIter i = opts.foreignKeys.begin(), e = opts.foreignKeys.end(); i != e; ++i) {
        KeyInfo ki;
        ki.map = make_shared<PopulationMap>(i->second + "/map.bin");
        ki.file = make_shared<MappedInputFile>(i->second + "/ids.bin");
        ki.fieldIndex = i->first;
        _foreignKeys.push_back(ki);
    }
    // Determine which chunks to generate data for
    if (opts.chunkIds.empty()) {
        _chunkIds = _chunker.getChunksFor(
            opts.dupRegion, opts.node, opts.numNodes, opts.hashChunks);
    } else {
        _chunkIds = opts.chunkIds;
    }
    cout << "Data for " << _chunkIds.size() << " chunks will be generated." << endl;
}

void ChunkDuplicator::duplicate() {
    cout << "Generating chunks..." << endl;
    typedef vector<int32_t>::const_iterator ChunkIdIter;
    for (ChunkIdIter i = _chunkIds.begin(), e = _chunkIds.end(); i != e; ++i) {
        cout << "\tchunk " << *i << "... " << flush;
        boost::timer t;
        SphericalBox box = _chunker.getChunkBounds(*i);
        box.expand(_opts.overlap + 1/3600.0); // 1 arcsecond epsilon
        _htmIds = box.htmIds(_opts.htmLevel);
        _chunkId = *i;
        generateChunk();
        finishChunk();
        cout << t.elapsed() << " sec" << endl;
    }
}

void ChunkDuplicator::generateChunk() {
    assert(_blocks.empty());
    // create thread pool
    int const numThreads = max(1, _opts.numThreads);
    boost::scoped_array<pthread_t> threads(new pthread_t[numThreads - 1]);
    for (int t = 1; t < numThreads; ++t) {
        if (::pthread_create(&threads[t-1], 0, &run, static_cast<void *>(this)) != 0) {
            perror("pthread_create() failed");
            exit(EXIT_FAILURE);
        }
    }
    run(static_cast<void *>(this));
    // wait for all threads to finish
    for (int t = 1; t < numThreads; ++t) {
        ::pthread_join(threads[t-1], 0);
    }
    assert(_htmIds.empty());
}

void ChunkDuplicator::finishChunk() {
    assert(!_blocks.empty());
    // setup chunk file writers
    scoped_ptr<BlockWriter> chunk;
    scoped_ptr<BlockWriter> selfOverlapChunk;
    scoped_ptr<BlockWriter> fullOverlapChunk;
    {
        std::ostringstream s;
        s << "_" << _chunkId << ".csv";
        string p = _opts.chunkDir + "/" + _opts.prefix;
        chunk.reset(new BlockWriter(p + s.str(), _opts.blockSize));
        if (_opts.overlap > 0.0) {
            selfOverlapChunk.reset(new BlockWriter(
                p + "SelfOverlap" + s.str(), _opts.blockSize));
            fullOverlapChunk.reset(new BlockWriter(
                p + "FullOverlap" + s.str(), _opts.blockSize));
        }
    }
    // merge sort output blocks
    vector<ChunkRecordRun> runs;
    runs.reserve(_blocks.size());
    typedef OutputBlockVector::const_iterator BlockIter;
    for (BlockIter i = _blocks.begin(), e = _blocks.end(); i != e; ++i) {
        ChunkRecordRun r;
        r.beg = (*i)->getRecords().begin();
        r.end = (*i)->getRecords().end();
        runs.push_back(r);
    }
    make_heap(runs.begin(), runs.end());
    while (!runs.empty()) {
        // get next record
        pop_heap(runs.begin(), runs.end());
        ChunkRecord const & rec = runs.back().get();
        // write record to appropriate files
        if (rec.loc.overlap == ChunkLocation::CHUNK) {
            chunk->append(rec.line, rec.length);
        } else {
            if (rec.loc.overlap == ChunkLocation::SELF_OVERLAP) {
                selfOverlapChunk->append(rec.line, rec.length);
            }
            fullOverlapChunk->append(rec.line, rec.length);
        }
        // advance / restore min-heap invariants
        if (runs.back().advance()) {
            runs.pop_back();
        } else {
            push_heap(runs.begin(), runs.end());
        }
    }
    // cleanup
    _blocks.clear();
}

void * ChunkDuplicator::run(void * arg) {
    ChunkDuplicator * d = static_cast<ChunkDuplicator *>(arg);
    TrixelDuplicator t(*d);
    t.duplicate();
    return 0;
}


TrixelDuplicator::TrixelDuplicator(ChunkDuplicator const & dup) :
    _dup(dup),
    _opts(_dup._opts),
    _inputLine(0),
    _inputEnd(0),
    _mapPositions(false),
    _posMapper(),
    _keyMappers(new KeyMapper[_dup._foreignKeys.size()]()),
    _chunkIdField(_opts.chunkIdField),
    _subChunkIdField(_opts.subChunkIdField),
    _numOutputFields(_opts.fields.size()),
    _fields(new char const *[_opts.fields.size() + 1]()),
    _values(new FieldValue[_opts.fields.size() + 2]()),
    _block(),
    _locations(),
    _madvStart(0),
    _madvSize(0)
{
    if (_chunkIdField == -1) {
        _chunkIdField = _numOutputFields++;
    }
    if (_subChunkIdField == -1) {
        _subChunkIdField = _numOutputFields++;
    }
}

void TrixelDuplicator::duplicate() {
    while (true) {
        uint32_t htmId;
        {
            Lock(_dup._mutex);
            if (_block && !_block->getRecords().empty()) {
                // store output block in queue
                _dup._blocks.push_back(_block);
            }
            if (_dup._htmIds.empty()) {
                break; // nothing left to do
            }
            // grab ID of next HTM trixel to fill with data
            htmId = _dup._htmIds.back();
            _dup._htmIds.pop_back();
        }
        setupTrixel(htmId);
        processTrixel();
        finishTrixel();
    }
}

void TrixelDuplicator::setupTrixel(uint32_t htmId) {
    // Allocate new output block
    _block.reset(new OutputBlock());
    // Map trixel to a non-empty source trixel
    uint32_t const sourceHtmId = _dup._primary.map->mapToNonEmptyTrixel(htmId);
    // Setup field remappers
    _mapPositions = (sourceHtmId != htmId);
    if (_mapPositions) {
        _posMapper = PosMapper(sourceHtmId, htmId);
    }
    _keyMappers[0] = KeyMapper(_dup._primary, sourceHtmId, htmId);
    for (size_t i = 1; i <= _dup._foreignKeys.size(); ++i) {
        _keyMappers[i] = KeyMapper(_dup._foreignKeys[i - 1], sourceHtmId, htmId);
    }
    // Locate data for the source trixel
    size_t off = static_cast<size_t>(_dup._primary.map->getOffset(sourceHtmId));
    size_t sz = static_cast<size_t>(_dup._primary.map->getSize(sourceHtmId));
    _inputLine = _dup._dataFile.data() + off;
    _inputEnd = _inputLine + sz;
    // Prefetch source trixel data
    size_t pageSize = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    size_t beg = off & ~(pageSize - 1);
    size_t end = (off + sz + pageSize - 1) & ~(pageSize - 1);
    _madvStart = const_cast<char *>(_dup._dataFile.data()) + beg;
    _madvSize = end - beg;
    madvise(_madvStart, _madvSize, MADV_WILLNEED); // or MADV_SEQUENTIAL?
}

void TrixelDuplicator::processTrixel() {
    typedef vector<ChunkLocation>::const_iterator ClIter;
    for (char const * next; _inputLine < _inputEnd; _inputLine = next) {
        // Clear out data from previous record
        _locations.clear();
        for (size_t i = 0; i < _numOutputFields; ++i) {
            _values[i].clear();
        }
        // Parse input line and extract partitioning ra, dec
        next = parseLine(_inputLine, _inputEnd, _opts.delimiter,
                         _fields.get(), _opts.fields.size());
        assert(next <= _inputEnd);
        Vector2d p;
        p(0) = extractDouble(_fields[_opts.partitionPos.first],
                             _fields[_opts.partitionPos.first + 1] - 1);
        p(1) = extractDouble(_fields[_opts.partitionPos.second],
                             _fields[_opts.partitionPos.second + 1] - 1);
        // map partitioning position to destination trixel if necessary
        if (_mapPositions) {
            p = _posMapper.map(p);
        }
        // find all locations of p
        _dup._chunker.locate(p, _dup._chunkId, _locations);
        if (_locations.empty()) {
            continue;
        }
        // p falls in the current chunk...
        if (_mapPositions) {
            // map ancillary positions
            _values[_opts.partitionPos.first].set(p(0));
            _values[_opts.partitionPos.second].set(p(1));
            for (size_t i = 0; i < _opts.positions.size(); ++i) {
                p(0) = extractDouble(_fields[_opts.positions[i].first],
                                     _fields[_opts.positions[i].first + 1] - 1);
                p(1) = extractDouble(_fields[_opts.positions[i].second],
                                     _fields[_opts.positions[i].second + 1] - 1);
                p = _posMapper.map(p);
                _values[_opts.positions[i].first].set(p(0));
                _values[_opts.positions[i].second].set(p(1));
            }
        }
        // map keys to destination trixel
        for (size_t i = 0; i < 1 + _opts.foreignKeys.size(); ++i) {
            int f = _keyMappers[i].getFieldIndex();
            int64_t id = extractInt(_fields[f], _fields[f + 1] - 1);
            id = _keyMappers[i].map(id);
            _values[f].set(id);
        }
        // extract secondary sort key
        int64_t sortKey = 0;
        if (_opts.secondarySortField >= 0) {
            sortKey = extractInt(_fields[_opts.secondarySortField],
                                 _fields[_opts.secondarySortField + 1] - 1);
        }
        _values[_chunkIdField].set(static_cast<int64_t>(_dup._chunkId));
        // loop over locations
        for (ClIter i = _locations.begin(), e = _locations.end(); i != e; ++i) {
            _values[_subChunkIdField].set(static_cast<int64_t>(i->subChunkId));
            ChunkRecord r;
            r.loc = *i;
            r.length = buildOutputLine();
            r.sortKey = sortKey;
            r.line = _outputLine;
            _block->add(r);
        }
    }
}

void TrixelDuplicator::finishTrixel() {
    madvise(_madvStart, _madvSize, MADV_DONTNEED);
}

uint32_t TrixelDuplicator::buildOutputLine() {
    char delim = _opts.delimiter;
    size_t i = 0, f = 0;
    for (; f < _opts.fields.size(); ++f) {
        if (f > 0) {
            _outputLine[i++] = delim;
        }
        char const * src;
        size_t len;
        if (_values[f].length > 0) {
            src = _values[f].buf;
            len = _values[f].length;
        } else {
            src = _fields[f];
            len = static_cast<size_t>(_fields[f + 1] - _fields[f]) - 1;
        }
        memcpy(_outputLine + i, src, len);
        i += len;
    }
    for (; f < _numOutputFields; ++f) {
        _outputLine[i++] = delim;
        size_t len = _values[f].length;
        memcpy(_outputLine + i, _values[f].buf, len);
        i += len;
    }
    _outputLine[i++] = '\n';
    assert(i <= static_cast<size_t>(MAX_LINE_SIZE));
    return static_cast<uint32_t>(i);
}

} // unnamed namespace


int main(int argc, char ** argv) {
    boost::timer total;
    Options options = parseDuplicatorCommandLine(argc, argv);
    ChunkDuplicator duplicator(options);
    duplicator.duplicate();
    cout << endl << "Duplicator finished in " << total.elapsed() << " sec" << endl;
    return EXIT_SUCCESS;
}

