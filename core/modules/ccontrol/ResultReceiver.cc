// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2014 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
#include "ccontrol/ResultReceiver.h"

// System headers
#include <cstring> // For memmove()

// Qserv headers
// #include "ccontrol/ConfigMap.h"
// #include "ccontrol/UserQuery.h"
// #include "ccontrol/userQueryProxy.h"
// #include "css/Facade.h"
#include "log/Logger.h"
// #include "qdisp/Executive.h"
// #include "qproc/QuerySession.h"
#include "rproc/TableMerger.h"
// #include "util/PacketBuffer.h"

namespace lsst {
namespace qserv {
namespace ccontrol {
int const ResultReceiver_bufferSize = 128*1024;

ResultReceiver::ResultReceiver(boost::shared_ptr<rproc::TableMerger> merger,
                               std::string const& tableName)
    : _merger(merger), _tableName(tableName),
      _actualSize(ResultReceiver_bufferSize),
      _flushed(false) {
    // Consider allocating buffer lazily, at first invocation of buffer()
    _actualBuffer.reset(new char[_actualSize]);
    assert(_actualBuffer);
    _buffer = _actualBuffer.get();
    _bufferSize = _actualSize;
}

int ResultReceiver::bufferSize() const {
    return _bufferSize;
}

char* ResultReceiver::buffer() {
    _flushed = false;
    return _buffer;
}

void ResultReceiver::flush(int bLen, bool last) {
    // Do something with the buffer.
    // FIXME
    // Pass to table merger.
//    _merger
    std::string tableName;
    assert(!tableName.empty());
    off_t inputSize = _buffer - _actualBuffer.get() + bLen;
    off_t mergeSize = _merger->merge(_actualBuffer.get(), inputSize,
                                     _tableName);
    if((mergeSize > 0) && !last) { // Something got merged.
        // Shift buffer contents to receive more.
        char* unMerged = _actualBuffer.get() + mergeSize;
        off_t unMergedSize = inputSize - mergeSize;
        std::memmove(_actualBuffer.get(), unMerged, unMergedSize);
        _buffer = _actualBuffer.get() + unMergedSize;
        _bufferSize = _actualSize - unMergedSize;
    } else if(mergeSize == 0) {
        LOGGER_ERR << "No merge in input. Receive buffer too small? "
                   << std::endl;
    }
    _flushed = true;
    if(last) {
        // Probably want to notify that we're done?
        LOGGER_INF << " Flushed last for tableName="
                   << _tableName << std::endl;
        if(_finishHook) {
            (*_finishHook)(true);
        }
    }
}

void ResultReceiver::errorFlush() {
    // Might want more info from result service.
    // Do something about the error. FIXME.
    LOGGER_ERR << "Error receiving result." << std::endl;
    if(_finishHook) {
        (*_finishHook)(false);
    }
}

bool ResultReceiver::finished() const {
    return _flushed;
}

#if 0
    explicit TableMerger(TableMergerConfig const& c);

    bool merge(std::string const& dumpFile, std::string const& tableName);
    bool merge2(std::string const& dumpFile, std::string const& tableName);

    // Fragmented merger
//    bool merge(PacketIterPtr pacIter, std::string const& tableName);
    bool merge(PacketBuffer::Ptr pacIter, std::string const& tableName);

    TableMergerError const& getError() const { return _error; }
    std::string getTargetTable() const {return _config.targetTable; }

    bool finalize();
#endif

}}} // lsst::qserv::ccontrol
