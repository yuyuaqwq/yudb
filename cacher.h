#pragma once

#include "page.h"
#include "lru_list.h"

namespace yudb {

using CacheId = uint32_t;

struct CacheInfo {
    uint32_t reference_count;
};

class Pager;

class Cacher {
public:
    Cacher(Pager* pager);
    ~Cacher();

    CacheId Alloc(PageId pgid) {

    }

    void Free(CacheId cache_id) {

    }

private:
    Pager* pager_;
    LruList<PageId, CacheInfo> lru_list_;
    uint8_t* page_pool_;
};

} // namespace yudb