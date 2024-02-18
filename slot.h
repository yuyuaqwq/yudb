#pragma once

#include <cstdint>
#include <cstring>
#include <cassert>

#include <utility>

#include "page_format.h"

namespace yudb {

using SlotId = uint16_t;

#pragma pack(push, 1)

struct Slot {
    uint16_t record_offset : 15;
    uint16_t overflow_page : 1;
    uint16_t key_length : 15;
    uint16_t bucket : 1;
    union {
        PageId left_child;
        uint32_t value_length;
    };
    static_assert(sizeof(left_child) == sizeof(value_length));
};

#pragma pack(pop)

} // namespace yudb 