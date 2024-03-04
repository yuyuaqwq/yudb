#pragma once

#include <iostream>
#include <array>
#include <unordered_map>

#include "yudb/cache.h"
#include "yudb/page_format.h"
#include "yudb/lru_list.h"
#include "yudb/noncopyable.h"

namespace yudb {

constexpr size_t kCacheFastMapPoolCount = 64;

class Pager;

class CacheManager : noncopyable {
public:
    explicit CacheManager(Pager* pager);
    ~CacheManager();

    std::pair<CacheInfo*, uint8_t*> Reference(PageId pgid);
    void AddReference(const uint8_t* page_cache);
    void Dereference(const uint8_t* page_cache);
    PageId GetPageIdByCache(const uint8_t* page_cache);

    auto& lru_list() const { return lru_list_; }
    auto& lru_list() { return lru_list_; }

private:
    CacheId GetCacheIdByCache(const uint8_t* page_cache);

private:
    Pager* const pager_;

    std::array<std::pair<PageId, CacheId>, kCacheFastMapPoolCount> fast_map_;

    LruList<PageId, CacheInfo> lru_list_;
    uint8_t* page_pool_;

#ifndef NDEBUG
    std::unordered_map<PageId, uint32_t> debug_page_crc32_map_;
#endif
};

} // namespace yudb