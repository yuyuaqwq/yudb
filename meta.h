#pragma once

#include <cstdint>

#include "page.h"
#include "txid.h"

namespace yudb {

#pragma pack(push, 1)
struct Meta {
    uint32_t sign;
    uint32_t min_version;
    PageSize page_size;
    PageCount page_count;
    PageId root;
    TxId txid;
    PageId free_db_root;
    uint32_t crc32;
};
#pragma pack(pop)

constexpr size_t kMetaSize = sizeof(Meta);

} // namespace yudb