#include "cacher.h"

#include "pager.h"

namespace yudb {

Cacher::Cacher(Pager* pager) :
    pager_ { pager },
    lru_list_{ kCacherPoolSize }
{
    page_pool_ = reinterpret_cast<uint8_t*>(operator new(pager->page_count() * pager->page_size()));
}

Cacher::~Cacher() {
    operator delete(page_pool_);
}

uint8_t* Cacher::Reference(PageId pgid) {
    auto [cache_info, cache_id] = lru_list_.Get(pgid);
    if (!cache_info) {
        lru_list_.Put(pgid, CacheInfo{0});
        auto pair = lru_list_.Get(pgid);
        cache_info = pair.first;
        cache_id = pair.second;
        auto cache = &page_pool_[cache_id * pager_->page_size()];

        pager_->Read(pgid, cache, 1);

        memset(cache, 0, pager_->page_size());
    }
    ++cache_info->reference_count;
    return &page_pool_[cache_id * pager_->page_size()];
}

PageId Cacher::CacheToPageId(uint8_t* page_cache) {
    auto diff = page_cache - page_pool_;
    CacheId cache_id = diff / pager_->page_size();
    auto cache_info = lru_list_.GetNodeByCacheId(cache_id);
    return cache_info.key;
}

void Cacher::Dereference(uint8_t* page_cache) {
    auto diff = page_cache - page_pool_;
    CacheId cache_id = diff / pager_->page_size();
    auto cache_info = &lru_list_.GetNodeByCacheId(cache_id).value;
    --cache_info->reference_count;
    assert(static_cast<int32_t>(cache_info->reference_count) >= 0);
}

} // namespace yudb