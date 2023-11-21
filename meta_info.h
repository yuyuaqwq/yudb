#pragma once

#include <cstdint>

#include "page.h"
#include "tx.h"

namespace yudb {

class Db;

#pragma pack(push, 1)
struct MetaInfo {
    uint32_t sign;
    uint32_t min_version;
    PageSize page_size;
    PageId root;
    TxId txid;
    uint32_t crc32;
};
#pragma pack(pop)

class MetaInfor {
public:
    MetaInfor() = default;

    bool Load();
private:
    Db* db_;
    MetaInfo meta_info_{ 0 };
    ptrdiff_t meta_index_{ 0 };
};

} // namespace yudb