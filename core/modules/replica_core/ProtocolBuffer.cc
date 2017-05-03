/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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

// Class header

#include "replica_core/ProtocolBuffer.h"

// System headers

// Qserv headers

namespace lsst {
namespace qserv {
namespace replica_core {

ProtocolBuffer::ProtocolBuffer (size_t capacity)
    :   _data    (new char[capacity]),
        _capacity(capacity),
        _size    (0)
{}

void
ProtocolBuffer::resize (size_t newSizeBytes) {

    if (newSizeBytes > _capacity)
        throw std::overflow_error("not enough buffer space to accomodate the request");

    _size = newSizeBytes;
}

uint32_t
ProtocolBuffer::parseLength () const {

    if (_size != sizeof(uint32_t))
        std::overflow_error("not enough data to be interpreted as the frame header");

    return ntohl(*(reinterpret_cast<const uint32_t*>(_data)));
}

}}} // namespace lsst::qserv::replica_core
