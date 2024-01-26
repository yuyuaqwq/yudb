#pragma once

#include "page.h"
#include "txid.h"
#include "page_space.h"

namespace yudb {

constexpr uint16_t kRecordInvalidIndex = 0xffff;

#pragma pack(push, 1)
struct FreeBlock {
    PageOffset next;
    uint16_t size;
};

struct BlockTableDescriptor {
    PageId pgid;
    uint16_t entry_index;
    uint16_t count;
};

struct BlockTableEntry {
    PageId pgid;
    uint16_t max_free_size;
};

struct BlockPage {
    union {
        struct {
            TxId last_modified_txid;
            PageSpace page_space;
            PageOffset first_block_pos;
            PageSize fragment_size;
        };
        uint8_t page[1];
    };
    BlockTableEntry block_table[1];
};



/*
* 极端场景下，每一个span都需要分配一页来存储
* 而每个leaf_element都有2个span，leaf_element_size是12字节
* 
* 需保证一页能够装入block_table
* 要求 node_size >= leaf_element_size(12) * 2 = 24 (4个span，4项entry)
* 即满足 block_page_header_size(16) + entry(6) = 22 所使用的空间 (4项entry)
*/

#pragma pack(pop)

} // namespace yudb