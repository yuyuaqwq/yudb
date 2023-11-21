#pragma once

#include "page.h"
#include "lru_list.h"

namespace yudb {

struct CacheInfo {
    int32_t reference_count;
    int32_t pool_index;
};

struct CachePage {
    CachePage* next;
};

class Db;

class Cacher {
public:
    

private:
    Db* db_;
    LruList<PageId, CacheInfo> lru_list_;
    uint8_t* page_pool_;
};

} // namespace yudb