#pragma once

#include <cstdint>

#include "page.h"
#include "tx_id.h"

namespace yudb {

#pragma pack(push, 1)
struct Meta {
    uint32_t sign;
    uint32_t min_version;
    PageSize page_size;
    PageCount page_count;
    PageId root;
    TxId tx_id;
    uint32_t crc32;
};
#pragma pack(pop)

} // namespace yudb