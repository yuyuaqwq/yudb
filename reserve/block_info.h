#pragma once

#include "page.h"
#include "txid.h"

namespace yudb {

constexpr uint16_t kRecordInvalidIndex = 0xffff;
constexpr PageOffset kFreeInvalidPos = 0xffff;

#pragma pack(push, 1)
struct FreeBlock {
    PageOffset next;
    uint16_t size;
};

struct BlockRecord {
    PageId pgid;
    TxId last_modified_txid;
    PageOffset first;
    uint16_t max_free_size;
};
#pragma pack(pop)

} // namespace yudb