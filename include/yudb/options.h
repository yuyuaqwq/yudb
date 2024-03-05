#pragma once

#include "yudb/page_format.h"

namespace yudb {

struct Options {
    const PageSize page_size{ 1024 };
    const size_t cache_pool_page_count{ 1024 };
    const size_t log_file_limit_bytes{ 1024 * 1024 * 64 };
};

} // namespace yudb