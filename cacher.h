#pragma once

#include "cache.h"
#include "page.h"
#include "lru_list.h"

namespace yudb {

class Pager;

class Cacher {
public:
    Cacher(Pager* pager);
    ~Cacher();

    CacheId Alloc(PageId pgid) {

    }

    void Free(CacheId cache_id) {

    }

    uint8_t* Reference(PageId pgid);

    void Dereference(uint8_t* page_cache);

private:
    Pager* pager_;
    LruList<PageId, CacheInfo> lru_list_;
    uint8_t* page_pool_;
};

} // namespace yudb