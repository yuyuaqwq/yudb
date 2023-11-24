#pragma once

#include <cstdint>

namespace yudb {

using CacheId = uint32_t;

constexpr CacheId  kCacheInvalidId = 0xffffffff;

struct CacheInfo {
    uint32_t reference_count;
};

} // namespace yudb