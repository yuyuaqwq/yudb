#pragma once

#include "page_format.h"

namespace yudb {

struct Options {
    const PageSize page_size{ 1024 };
    const size_t cache_page_pool_count{ 0x1000 };
    //const size_t cache_fast_map_pool_count{ 64 };
};

} // namespace yudb