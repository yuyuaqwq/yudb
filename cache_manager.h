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

    std::pair<CacheInfo*, uint8_t*> Reference(PageId pgid);
    void Dereference(uint8_t* page_cache);
    PageId CacheToPageId(uint8_t* page_cache);

    auto& lru_list() const { return lru_list_; }
    auto& lru_list() { return lru_list_; }

private:
    Pager* const pager_;
    LruList<PageId, CacheInfo> lru_list_;
    uint8_t* page_pool_;
};

} // namespace yudb