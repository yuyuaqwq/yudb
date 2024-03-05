#pragma once

#include <cstdint>
#include <cstring>
#include <cassert>

#include <utility>

#include "yudb/page_format.h"

namespace yudb {

using SlotId = uint16_t;
constexpr SlotId kSlotInvalidId = 0xffff;

#pragma pack(push, 1)
struct Slot {
    uint16_t record_offset : 15;
    uint16_t is_overflow_pages : 1;
    uint16_t key_length;
    union {
        PageId left_child;
        uint32_t value_length;
        static_assert(sizeof(left_child) == sizeof(value_length));
    };
};
#pragma pack(pop)

} // namespace yudb 