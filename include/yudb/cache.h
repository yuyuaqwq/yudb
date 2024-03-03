#pragma once

#include <cstdint>

namespace yudb {

using CacheId = uint32_t;

constexpr CacheId kCacheInvalidId = 0xffffffff;

struct CacheInfo {
    uint32_t reference_count : 31;
    uint32_t dirty : 1;
};

} // namespace yudb