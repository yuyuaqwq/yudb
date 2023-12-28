#pragma once

#include <cstdint>

namespace yudb {

using PageId = uint32_t;
using PageCount = uint32_t;

using PageSize = uint16_t;
using PageOffset = int16_t;

constexpr PageSize kPageSize = 100;

constexpr PageId kPageInvalidId = 0xffffffff;

#pragma pack(push, 1)
struct PageHeader {
    
};
#pragma pack(pop)

//auto test = sizeof(Page::Cell);

} // namespace yudb