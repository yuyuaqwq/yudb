#pragma once

#include <cstdint>

namespace yudb {

using PageId = uint32_t;
using PageCount = uint32_t;

using PageSize = uint16_t;
using PageOffset = int16_t;

constexpr PageOffset kPageInvalidOffset = 0xffff;
constexpr PageSize kPageMinSize = 0x100;
constexpr PageSize kPageMaxSize = 0x8000;
constexpr PageId kPageInvalidId = 0xffffffff;
constexpr PageCount kPageMaxCount = 0x80000000;

} // namespace yudb