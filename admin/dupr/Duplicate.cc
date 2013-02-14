#include <sys/types.h>
#include <stdint.h>
#include <stdio.h>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "boost/make_shared.hpp"
#include "boost/ref.hpp"
#include "boost/scoped_array.hpp"
#include "boost/scoped_ptr.hpp"
#include "boost/thread.hpp"
#include "boost/timer/timer.hpp"

#include "Block.h"
#include "Csv.h"
#include "Geometry.h"
#include "HtmIndex.h"
#include "Options.h"


using std::cerr;
using std::cout;
using std::endl;
using std::flush;
using std::max;
using std::pair;
using std::string;
using std::vector;

using boost::make_shared;
using boost::scoped_ptr;
using boost::scoped_array;
using boost::shared_ptr;
using boost::timer::cpu_timer;


using namespace dupr;


namespace {

struct KeyInfo {
    shared_ptr<HtmIndex const> idx;
    shared_ptr<InputFile const> file; // File containing ID/key values.
    int fieldIndex;                   // ID/key field index.

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

    ~KeyMapper();

    void setup(KeyInfo const & key, uint32_t srcHtmId, uint32_t dstHtmId);

    int getFieldIndex() const {
        return _fieldIndex;
    }

    int64_t map(int64_t key) const {
        int64_t const * p = std::lower_bound(_beg, _end, key);
        assert(p != _end && key == *p);
        return (static_cast<int64_t>(_htmId) << 32) + (p - _beg);
    }

private:
    KeyMapper(KeyMapper const & mapper);
    KeyMapper & operator=(KeyMapper const & mapper);

    int64_t * _beg;
    int64_t * _end;
    uint32_t _htmId;
    int _fieldIndex;
};

void KeyMapper::setup(KeyInfo const & key, uint32_t srcHtmId, uint32_t dstHtmId) {
    free(_beg);
    _beg = 0;
    _end = 0;
    _htmId = dstHtmId;
    _fieldIndex = key.fieldIndex;
    uint64_t offset = key.map->getNumRecordsBelow(srcHtmId) * sizeof(int64_t);
    uint32_t nrec = key.map->getNumRecords(srcHtmId);
    _beg = static_cast<int64_t *>(key.file->read(
        0, static_cast<off_t>(offset), nrec * sizeof(int64_t)));
    _end = _beg + nrec;
}

KeyMapper::~KeyMapper() {
    free(_beg);
    _beg = 0;
    _end = 0;
}


/** Remaps positions from a source HTM trixel to a destination HTM trixel.
  */
class PosMapper {
public:
    PosMapper() : _m(Matrix3d::Identity()) { }

    PosMapper(uint32_t sourceHtmId,
              uint32_t destinationHtmId);

    pair<double, double> const map(pair<double, double> const & pos) const {
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
    typedef vector<char *>::const_iterator Iter;
    for (Iter i = _lineBlocks.begin(), e = _lineBlocks.end(); i != e; ++i) {
        free(*i);
    }
}

void OutputBlock::add(ChunkRecord const & r) {
    assert(r.length < static_cast<uint32_t>(MAX_LINE_SIZE));
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
        if (val != val || (val != 0.0 && val == 2.0*val)) {
            buf[0] = '\\';
            buf[1] = 'N';
            length = 2;
        } else {
            int n = snprintf(buf, sizeof(buf), "%.17g", val);
            assert(n > 0 && n < static_cast<int>(sizeof(buf) - 1));
            length = static_cast<unsigned char>(n);
        }
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
    ~TrixelDuplicator();

    void duplicate();

private:
    TrixelDuplicator(TrixelDuplicator const &);
    TrixelDuplicator & operator=(TrixelDuplicator const &);

    void setupTrixel(uint32_t htmId);
    void processTrixel();
    uint32_t buildOutputLine();

    ChunkDuplicator const &    _dup;
    Options const &            _opts;
    char *                     _inputStart;
    char const *               _inputLine;
    char const *               _inputEnd;
    bool                       _mapPositions;
    PosMapper                  _posMapper;
    KeyMapper                  _pkMapper;
    KeyMapper                  _fkMapper;
    int                        _chunkIdField;
    int                        _subChunkIdField;
    size_t                     _numOutputFields;
    scoped_array<char const *> _fields;
    scoped_array<FieldValue>   _values;
    shared_ptr<OutputBlock>    _block;
    vector<ChunkLocation>      _locations;
    char                       _outputLine[MAX_LINE_SIZE];
};


/** Duplicator that handles a single chunk at a time.
  */
class ChunkDuplicator {
public:
    ChunkDuplicator(Options const & options);

    void duplicate();

    void operator()() {
        try {
            TrixelDuplicator t(*this);
            t.duplicate();
        } catch (std::exception const & ex) {
            cerr << ex.what() << endl;
            exit(EXIT_FAILURE);
        }
    }

private:
    ChunkDuplicator(ChunkDuplicator const &);
    ChunkDuplicator & operator=(ChunkDuplicator const &);

    void generateChunk();
    void finishChunk();

    Options const &   _opts;
    Chunker           _chunker;
    KeyInfo           _primary;
    KeyInfo           _foreign;
    InputFile         _dataFile;
    vector<int32_t>   _chunkIds;
    int32_t           _chunkId;

    char                      _cl0[CACHE_LINE_SIZE];

    mutable boost::mutex      _mutex;
    mutable vector<uint32_t>  _htmIds;
    mutable OutputBlockVector _blocks;

    char                      _cl1[CACHE_LINE_SIZE];

    friend class TrixelDuplicator;
};

ChunkDuplicator::ChunkDuplicator(Options const & opts) :
    _opts(opts),
    _chunker(opts.overlap, opts.numStripes, opts.numSubStripesPerStripe),
    _primary(),
    _foreign(),
    _dataFile(opts.indexDir + "/data.csv"),
    _chunkIds(),
    _chunkId(-1),
    _mutex(),
    _htmIds(),
    _blocks()
{
    // Read in population maps, and memory-map required CSV/id files
    _primary.idx = make_shared<HtmIndex>(opts.indexDir + "/map.bin");
    _primary.file = make_shared<InputFile>(opts.indexDir + "/ids.bin");
    _primary.fieldIndex = opts.pkField;
    if (opts.fkField != -1) {
        _foreign.idx = make_shared<HtmIndex>(opts.fkIndexDir + "/map.bin");
        _foreign.file = make_shared<InputFile>(opts.fkIndexDir + "/ids.bin");
        _foreign.fieldIndex = opts.fkField;
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
    typedef vector<int32_t>::const_iterator Iter;
    for (Iter i = _chunkIds.begin(), e = _chunkIds.end(); i != e; ++i) {
        cout << "\tchunk " << *i << "...\n" << flush;
        cpu_timer t;
        SphericalBox box = _chunker.getChunkBounds(*i);
        box.expand(_opts.overlap + 1/3600.0); // 1 arcsecond epsilon
        _htmIds = box.htmIds(_opts.htmLevel);
        _chunkId = *i;
        generateChunk();
        cout << "\t\tgeneration : " << t.format() << flush;
        cpu_timer t2;
        finishChunk();
        cout << "\t\tsort/write : " << t2.format() << flush;
    }
}

void ChunkDuplicator::generateChunk() {
    assert(_blocks.empty());
    // create thread pool
    int const numThreads = max(1, _opts.numThreads);
    boost::scoped_array<boost::thread> threads(new boost::thread[numThreads - 1]);
    for (int t = 0; t < numThreads - 1; ++t) {
        threads[t] = boost::thread(boost::ref(*this));
    }
    this->operator()();
    // wait for all threads to finish
    for (int t = 0; t < numThreads - 1; ++t) {
        threads[t].join();
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
    typedef OutputBlockVector::const_iterator Iter;
    for (Iter i = _blocks.begin(), e = _blocks.end(); i != e; ++i) {
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


TrixelDuplicator::TrixelDuplicator(ChunkDuplicator const & dup) :
    _dup(dup),
    _opts(_dup._opts),
    _inputStart(0),
    _inputLine(0),
    _inputEnd(0),
    _mapPositions(false),
    _posMapper(),
    _pkMapper(),
    _fkMapper(),
    _chunkIdField(_opts.chunkIdField),
    _subChunkIdField(_opts.subChunkIdField),
    _numOutputFields(_opts.fields.size()),
    _fields(new char const *[_opts.fields.size() + 1]()),
    _values(new FieldValue[_opts.fields.size() + 2]()),
    _block(),
    _locations()
{
    if (_chunkIdField == -1) {
        _chunkIdField = _numOutputFields++;
    }
    if (_subChunkIdField == -1) {
        _subChunkIdField = _numOutputFields++;
    }
}

TrixelDuplicator::~TrixelDuplicator() {
    free(_inputStart);
}

void TrixelDuplicator::duplicate() {
    while (true) {
        uint32_t htmId;
        {
            boost::lock_guard<boost::mutex> lock(_dup._mutex);
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
    }
}

void TrixelDuplicator::setupTrixel(uint32_t htmId) {
    // Free previous input data and allocate new output block
    free(_inputStart);
    _block.reset(new OutputBlock());
    // Map trixel to a non-empty source trixel
    uint32_t sourceHtmId;
    if (_dup._foreign.fieldIndex != -1) {
        sourceHtmId = _dup._foreign.idx->mapToNonEmpty(htmId).id;
    } else {
        sourceHtmId = _dup._primary.idx->mapToNonEmpty(htmId).id;
    }
    // Setup field remappers
    _mapPositions = (sourceHtmId != htmId);
    if (_mapPositions) {
        _posMapper = PosMapper(sourceHtmId, htmId);
    }
    _pkMapper.setup(_dup._primary, sourceHtmId, htmId);
    if (_dup._foreign.fieldIndex != -1) {
        _fkMapper.setup(_dup._foreign, sourceHtmId, htmId);
    }
    // Locate data for the source trixel
    uint64_t offset = _dup._primary.map->getOffset(sourceHtmId);
    uint32_t sz = _dup._primary.map->getSize(sourceHtmId);
    _inputStart = static_cast<char *>(_dup._dataFile.read(
        0, static_cast<off_t>(offset), sz));
    _inputLine = _inputStart;
    _inputEnd = _inputStart + sz;
}

void TrixelDuplicator::processTrixel() {
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
        pair<double, double> p;
        int ra = _opts.partitionPos.first;
        int dec = _opts.partitionPos.second;
        p.first = extractDouble(_fields[ra], _fields[ra + 1] - 1, false);
        p.second = extractDouble(_fields[dec], _fields[dec + 1] - 1, false);
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
            _values[ra].set(p.first);
            _values[dec].set(p.second);
            for (size_t i = 0; i < _opts.positions.size(); ++i) {
                ra = _opts.positions[i].first;
                dec = _opts.positions[i].second;
                p.first = extractDouble(_fields[ra], _fields[ra + 1] - 1, true);
                p.second = extractDouble(_fields[dec], _fields[dec + 1] - 1, true);
                p = _posMapper.map(p);
                _values[ra].set(p.first);
                _values[dec].set(p.second);
            }
        }
        // map keys to destination trixel
        int f = _pkMapper.getFieldIndex();
        if (!isNull(_fields[f], _fields[f + 1] - 1)) {
            int64_t id = extractInt(_fields[f], _fields[f + 1] - 1);
            id = _pkMapper.map(id);
            _values[f].set(id);
        }
        f = _fkMapper.getFieldIndex();
        if (f >= 0 && !isNull(_fields[f], _fields[f + 1] - 1)) {
            int64_t id = extractInt(_fields[f], _fields[f + 1] - 1); 
            id = _fkMapper.map(id);
            _values[f].set(id);
        }
        // extract secondary sort key
        int64_t sortKey = 0;
        f = _opts.secondarySortField;
        if (f >= 0 && !isNull(_fields[f], _fields[f + 1] - 1)) {
            sortKey = extractInt(_fields[f], _fields[f + 1] - 1);
        }
        _values[_chunkIdField].set(static_cast<int64_t>(_dup._chunkId));
        // loop over locations
        typedef vector<ChunkLocation>::const_iterator Iter;
        for (Iter i = _locations.begin(), e = _locations.end(); i != e; ++i) {
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
    try {
        cpu_timer total;
        Options options = parseDuplicatorCommandLine(argc, argv);
        ChunkDuplicator duplicator(options);
        duplicator.duplicate();
        cout << endl << "Duplicator finished : " << total.format() << endl;
    } catch (std::exception const & ex) {
        cerr << ex.what() << endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

