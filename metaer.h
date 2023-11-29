#pragma once

#include <cstdint>

#include "noncopyable.h"
#include "page.h"
#include "tx.h"

namespace yudb {

class Db;

#pragma pack(push, 1)
struct Meta {
    uint32_t sign;
    uint32_t min_version;
    PageSize page_size;
    PageCount page_count;
    PageId root;
    TxId txid;
    uint32_t crc32;
};
#pragma pack(pop)

class Metaer : noncopyable {
public:
    Metaer(Db* db) : db_{ db } {};

    bool Load();

    Meta& meta() { return meta_; }

private:
    Db* db_;
    Meta meta_{ 0 };
    ptrdiff_t meta_index_{ 0 };
};

} // namespace yudb