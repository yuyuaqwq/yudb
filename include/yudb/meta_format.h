#pragma once

#include <cstdint>

#include "yudb/page_format.h"
#include "yudb/tx_format.h"

namespace yudb {

#pragma pack(push, 1)
struct MetaStruct {
    uint32_t sign;
    uint32_t min_version;
    PageSize page_size;
    PageCount page_count;
    PageId user_root;
    PageId free_list_pgid;
    uint32_t free_pair_count;
    PageCount free_list_page_count;
    TxId txid;
    uint32_t crc32;
};
#pragma pack(pop)

constexpr size_t kMetaSize = sizeof(MetaStruct);

inline void CopyMetaInfo(MetaStruct* dst, const MetaStruct& src) {
    dst->user_root = src.user_root;
    dst->free_list_pgid = src.free_list_pgid;
    dst->free_pair_count = src.free_pair_count;
    dst->free_list_page_count = src.free_list_page_count;
    dst->page_count = src.page_count;
    dst->txid = src.txid;
}

} // namespace yudb