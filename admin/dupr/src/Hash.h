/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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

/// \file
/// \brief Hashing functions.

#ifndef LSST_QSERV_ADMIN_DUPR_HASH_H
#define LSST_QSERV_ADMIN_DUPR_HASH_H

#include <stdint.h>

namespace lsst { namespace qserv { namespace admin { namespace dupr {

/// 32-bit integer hash function from Brett Mulvey.
/// See http://home.comcast.net/~bretm/hash/4.html
inline uint32_t mulveyHash(uint32_t x) {
    x += x << 16;
    x ^= x >> 13;
    x += x << 4;
    x ^= x >> 7;
    x += x << 10;
    x ^= x >> 5;
    x += x << 8;
    x ^= x >> 16;
    return x;
}

}}}} // namespace lsst::qserv::admin::dupr

#endif // LSST_QSERV_ADMIN_DUPR_HASH_H
