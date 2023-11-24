#include "cacher.h"

#include "pager.h"

namespace yudb {

Cacher::Cacher(Pager* pager) :
    pager_ { pager },
    lru_list_{ pager->page_count() }
{
    page_pool_ = reinterpret_cast<uint8_t*>(operator new(pager->page_count() * pager->page_size()));
}

Cacher::~Cacher() {
    operator delete(page_pool_);
}

uint8_t* Cacher::Reference(PageId pgid) {
    auto [cache_info, cache_id] = lru_list_.Get(pgid);
    ++cache_info->reference_count;
    return &page_pool_[cache_id * pager_->page_size()];
}

void Cacher::Dereference(uint8_t* page_cache) {
    auto diff = page_cache - page_pool_;
    CacheId cache_id = diff / pager_->page_size();
    auto cache_info = lru_list_.GetByCacheId(cache_id);
    --cache_info->reference_count;
    assert(static_cast<int32_t>(cache_info->reference_count) >= 0);
}

} // namespace yudb