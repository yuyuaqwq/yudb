#include "cacher.h"

#include "pager.h"

namespace yudb {

Cacher::Cacher(Pager* pager) :
    pager_ { pager },
    lru_list_{ kCacherPoolPageCount }
{
    page_pool_ = reinterpret_cast<uint8_t*>(operator new(kCacherPoolPageCount * pager->page_size()));
}

Cacher::~Cacher() {
    if (page_pool_) {
        operator delete(page_pool_);
        page_pool_ = nullptr;
    }
}

std::pair<CacheInfo*, uint8_t*> Cacher::Reference(PageId pgid) {
    auto [cache_info, cache_id] = lru_list_.Get(pgid);
    if (!cache_info) {
        auto evict = lru_list_.Put(pgid, CacheInfo{0});
        if (evict) {
            // 将淘汰页面写回磁盘，未来添加写盘队列则直接放入队列
            auto& [evict_cache_id, evict_pgid, evict_cache_info] = *evict;
            if (evict_cache_info.reference_count != 0) {
                throw std::runtime_error("unable to reallocate cache.");
            }
            if (evict_cache_info.dirty) {
                pager_->Write(evict_pgid, &page_pool_[evict_cache_id * pager_->page_size()], 1);
            }
        }
        auto pair = lru_list_.Get(pgid);
        cache_info = pair.first;
        cache_info->dirty = false;
        cache_id = pair.second;
        auto cache = &page_pool_[cache_id * pager_->page_size()];

        pager_->Read(pgid, cache, 1);
    }
    ++cache_info->reference_count;
    return { cache_info, &page_pool_[cache_id * pager_->page_size()] };
}

void Cacher::Dereference(uint8_t* page_cache) {
    auto diff = page_cache - page_pool_;
    CacheId cache_id = diff / pager_->page_size();
    auto cache_info = &lru_list_.GetNodeByCacheId(cache_id).value;
    --cache_info->reference_count;
    assert(static_cast<int32_t>(cache_info->reference_count) >= 0);
}


PageId Cacher::CacheToPageId(uint8_t* page_cache) const {
    auto diff = page_cache - page_pool_;
    CacheId cache_id = diff / pager_->page_size();
    auto& cache_info = lru_list_.GetNodeByCacheId(cache_id);
    return cache_info.key;
}

} // namespace yudb