#pragma once

#include "yudb/page_format.h"

namespace yudb {

struct Options {
    const PageSize page_size{ 1024 };
    const size_t cache_pool_page_count{ 0x1000 };
    const size_t log_file_max_bytes;
};

} // namespace yudb