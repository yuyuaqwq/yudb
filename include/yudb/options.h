#pragma once

#include "yudb/page_format.h"
#include "yudb/comparator.h"

namespace yudb {

struct Options {
    //const PageSize page_size{ 1024 };
    //const size_t cache_pool_page_count{ 1024 };
    const size_t map_size{ 1024 * 1024 };
    const size_t log_file_limit_bytes{ 1024 * 1024 * 64 };
    Comparator defaluit_comparator{ ByteArrayComparator };
};

} // namespace yudb