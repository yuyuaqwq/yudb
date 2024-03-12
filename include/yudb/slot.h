#pragma once

#include <cstdint>
#include <cstring>
#include <cassert>

#include <utility>

#include "yudb/page_format.h"

namespace yudb {

using SlotId = int16_t;
constexpr SlotId kSlotInvalidId = -1;

constexpr size_t kKeyMaxSize = 0x7fff;
constexpr size_t kValueMaxSize = 0xffffffff;

#pragma pack(push, 1)
struct Slot {
    uint16_t record_offset : 15;
    uint16_t is_overflow_pages : 1;
    uint16_t key_size : 15;
    uint16_t is_bucket : 1;
    union {
        PageId left_child;
        uint32_t value_size;
        static_assert(sizeof(left_child) == sizeof(value_size));
    };
};
#pragma pack(pop)

} // namespace yudb 