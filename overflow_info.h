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

struct OverflowPage {
    TxId last_modified_txid;
    PageOffset first;
    uint16_t pedding;
    uint8_t data[];
};

struct OverflowRecord {
    PageId pgid;
    uint16_t max_free_size;
};

struct OverflowInfo {
    PageId record_pgid;
    TxId last_modified_txid;
    PageOffset record_offset;
    uint16_t record_index;
    uint16_t record_count;
    uint32_t pedding;
};

/*
* 极端场景下，每一个span都需要分配一页来存储
* 而每个leaf_element都有2个span，leaf_element是12字节

* record_count = (page_size - overflow_page_header_size) / recoud_size
* record至多需要一页来存，因此 record_count -= 1
* (page_size - node_header_size) / leaf_element_size * 2 <= record_count
* 
* 要即可保证一页能够装入record_arr
* 需要保证 noder_size >= leaf_element_size * 2 (最大占用4页，即4项record)
* 抵消掉 overflow_page_header_size(8) + record_arr_page(6) 所占用的空间(3项record)
*/

#pragma pack(pop)

} // namespace yudb