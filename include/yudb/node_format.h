#pragma once

#include "yudb/tx_format.h"
#include "yudb/page_format.h"
#include "yudb/slot.h"

namespace yudb {

#pragma pack(push, 1)
enum class NodeType : uint16_t {
    kInvalid = 0,
    kBranch,
    kLeaf,
};

struct OverflowRecord {
    PageId pgid;
};

struct NodeHeader {
    TxId last_modified_txid;
    NodeType type : 2;
    uint16_t count : 14;  // count of slots.
    uint16_t space_used;  // excluding the space occupied by deleted records.
    uint16_t data_offset; // the tail of the records.
};

struct NodeStruct {
    NodeHeader header;
    union {
        PageId tail_child;
        uint32_t padding;
        static_assert(sizeof(tail_child) == sizeof(padding));
    };
    Slot slots[1];
};
#pragma pack(pop)

} // namespace