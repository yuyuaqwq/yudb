#pragma once

#include <cstdint>

#include "page_format.h"
#include "tx_format.h"

namespace yudb {

#pragma pack(push, 1)
struct MetaFormat {
    uint32_t sign;
    uint32_t min_version;
    PageSize page_size;
    PageCount page_count;
    PageId root;
    TxId txid;
    uint32_t crc32;
};
#pragma pack(pop)

constexpr size_t kMetaSize = sizeof(MetaFormat);

} // namespace yudb