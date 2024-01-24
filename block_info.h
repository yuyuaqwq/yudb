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

struct BlockPage {
    TxId last_modified_txid;
    PageSpace page_space;
    PageOffset first;
    uint16_t pedding;
    uint8_t data[1];
};

struct BlockRecord {
    PageId pgid;
    uint16_t max_free_size;
};

struct BlockInfo {
    PageId record_pgid;
    uint16_t record_index;
    uint16_t record_count;
};

/*
* 极端场景下，每一个span都需要分配一页来存储
* 而每个leaf_element都有2个span，leaf_element_size是12字节
* 
* 需保证一页能够装入record_arr
* 要求 node_size >= leaf_element_size(12) * 2 = 24 (4个span，4项record)
* 即满足 block_page_header_size(16) + record_arr_page(6) = 22 所使用的空间 (4项record)
*/

#pragma pack(pop)

} // namespace yudb