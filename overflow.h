#pragma once

#include "page.h"

namespace yudb {

constexpr uint16_t kRecordInvalidIndex = 0xffff;
constexpr PageOffset kFreeInvalidPos = 0xffff;

#pragma pack(push, 1)
struct FreeList{
    struct Block {
        PageOffset next;
        uint16_t size;
    };

    PageOffset first;
    uint16_t max_free_size;
};

struct Overflow {
    struct Record {
        PageId pgid;
        FreeList free_list;
    };

    PageId record_pgid;
    PageOffset record_offset;
    uint16_t record_index;
    uint16_t record_count;
};
#pragma pack(pop)

} // namespace yudb