#pragma once

#include "noncopyable.h"
#include "cache.h"
#include "page.h"
#include "lru_list.h"

namespace yudb {

constexpr size_t kCacherPoolSize = 0x1000;

class Pager;

class Cacher : noncopyable {
public:
    Cacher(Pager* pager);
    ~Cacher();

    uint8_t* Reference(PageId pgid);

    void Dereference(uint8_t* page_cache);

private:
    Pager* pager_;
    LruList<PageId, CacheInfo> lru_list_;
    uint8_t* page_pool_;
};

} // namespace yudb