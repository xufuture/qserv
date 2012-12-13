#ifndef HASH_H
#define HASH_H

#include <stdint.h>

namespace dupr {

/// 32 bit integer hash function from Brett Mulvey. See http://home.comcast.net/~bretm/hash/4.html
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

} // namespace dupr

#endif // HASH_H
