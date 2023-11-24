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

    uint8_t* Reference(PageId pgid) {
        auto [cache_info, cache_id] = lru_list_.Get(pgid);
        ++cache_info->reference_count;
        return &page_pool_[cache_id * pager_->page_size()];
    }

    void Dereference(uint8_t* page_cache) {
        auto diff = page_cache - page_pool_;
        CacheId cache_id = diff / pager_->page_size();
        auto cache_info = lru_list_.GetByCacheId(cache_id);
        --cache_info->reference_count;
        assert(static_cast<int32_t>(cache_info->reference_count) >= 0);
    }

private:
    Pager* pager_;
    LruList<PageId, CacheInfo> lru_list_;
    uint8_t* page_pool_;
};

} // namespace yudb