#pragma once

#include <iostream>

#include "noncopyable.h"
#include "cache.h"
#include "page_format.h"
#include "lru_list.h"

namespace yudb {

constexpr size_t kCachePoolPageCount = 0x1000;

class Pager;

class CacheManager : noncopyable {
public:
    explicit CacheManager(Pager* pager);

    ~CacheManager();

    CacheManager(CacheManager&& right) noexcept :
        pager_{ right.pager_ },
        lru_list_{ std::move(right.lru_list_) },
        page_pool_{ right.page_pool_ }
    {
        right.page_pool_ = nullptr;
    }
    void operator=(CacheManager&& right) noexcept {
        pager_ = right.pager_;
        lru_list_ = std::move(right.lru_list_);
        right.page_pool_ = nullptr;
        page_pool_ = right.page_pool_;
    }

    void set_pager(Pager* pager) {
        pager_ = pager;
    }

    std::pair<CacheInfo*, uint8_t*> Reference(PageId pgid);

    void Dereference(uint8_t* page_cache);

    PageId CacheToPageId(uint8_t* page_cache) const;

private:
    Pager* pager_;
    LruList<PageId, CacheInfo> lru_list_;
    uint8_t* page_pool_;
};

} // namespace yudb