#pragma once

#include "yudb/page_format.h"
#include "yudb/comparator.h"

namespace yudb {

struct Options {
    PageSize page_size{ 0 };
    const size_t log_file_limit_bytes{ 1024 * 1024 * 64 };
    const Comparator defaluit_comparator{ ByteArrayComparator };
};

} // namespace yudb