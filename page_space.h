#pragma once

#include "page.h"

namespace yudb {

struct PageSpace {
    PageSize rest_size;
    PageOffset right_size;
};

} // namespace yudb