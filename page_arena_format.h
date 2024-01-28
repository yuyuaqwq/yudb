#pragma once

#include "page_format.h"

namespace yudb {

struct PageArenaFormat {
    PageSize rest_size;
    PageOffset right_size;
};

} // namespace yudb