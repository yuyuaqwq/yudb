#pragma once

#include <cstdint>

namespace yudb {

using PageId = uint32_t;
using PageCount = uint32_t;

using PageSize = uint16_t;
using PageOffset = int16_t;

constexpr PageSize kPageSize = 100;

constexpr PageId kPageInvalidId = 0xffffffff;

} // namespace yudb