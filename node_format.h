#pragma once

#include "tx_format.h"
#include "page_format.h"
#include "slot.h"

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

struct LeafNodeFormat {
    NodeHeader header;
    Slot slots[1];

};

struct BranchNodeFormat {
    NodeHeader header;
    PageId tail_child;
    Slot slots[1];
};
#pragma pack(pop)

//static_assert(sizeof(NodeFormat) - sizeof(NodeFormat::body) >= sizeof(NodeFormat::LeafElement) * 2, "abnormal length of head node.");

} // namespace