#include "cache_manager.h"

#include "pager.h"
#include "db_impl.h"

namespace yudb {

CacheManager::CacheManager(Pager* pager) :
    pager_ { pager },
    lru_list_{ pager_->db().options()->cache_page_pool_count }
{
    for (auto& val : fast_map_) {
        val.first = kPageInvalidId;
    }
    page_pool_ = reinterpret_cast<uint8_t*>(operator new(pager_->db().options()->cache_page_pool_count * pager->page_size()));
}

CacheManager::~CacheManager() {
    if (page_pool_) {
        operator delete(page_pool_);
        page_pool_ = nullptr;
    }
}

std::pair<CacheInfo*, uint8_t*> CacheManager::Reference(PageId pgid) {
    auto fast_map_index = pgid % kCacheFastMapPoolCount;
    if (fast_map_[fast_map_index].first == pgid) {
        const auto cache_id = fast_map_[fast_map_index].second;
        auto cache_info = &lru_list_.GetNodeByCacheId(cache_id).value;
        ++cache_info->reference_count;
        return { cache_info, &page_pool_[cache_id * pager_->page_size()] };
    } else {
        fast_map_[fast_map_index].first = pgid;
    }

    auto [cache_info, cache_id] = lru_list_.Get(pgid);
    if (!cache_info) {
        const auto evict = lru_list_.Put(pgid, CacheInfo{0});
        if (evict) {
            // 将淘汰页面写回磁盘，未来添加写盘队列则直接放入队列
            const auto& [evict_cache_id, evict_pgid, evict_cache_info] = *evict;
            if (evict_cache_info.reference_count != 0) {
                throw std::runtime_error("unable to reallocate cache.");
            }
            if (evict_cache_info.dirty) {
                pager_->Write(evict_pgid, &page_pool_[evict_cache_id * pager_->page_size()], 1);
            }
        }
        const auto pair = lru_list_.Get(pgid);
        cache_info = pair.first;
        cache_info->dirty = false;
        cache_id = pair.second;
        auto cache = &page_pool_[cache_id * pager_->page_size()];
        pager_->Read(pgid, cache, 1);
    }

    fast_map_[fast_map_index].second = cache_id;

    ++cache_info->reference_count;
    return { cache_info, &page_pool_[cache_id * pager_->page_size()] };
}

void CacheManager::AddReference(const uint8_t* page_cache) {
    auto cache_id = PageCacheToCacheId(page_cache);
    auto& cache_info = lru_list_.GetNodeByCacheId(cache_id).value;
    ++cache_info.reference_count;
}

void CacheManager::Dereference(const uint8_t* page_cache) {
    auto cache_id = PageCacheToCacheId(page_cache);
    auto& cache_info = lru_list_.GetNodeByCacheId(cache_id).value;
    assert(cache_info.reference_count > 0);
    --cache_info.reference_count;
}

PageId CacheManager::CacheToPageId(const uint8_t* page_cache) {
    auto cache_id = PageCacheToCacheId(page_cache);
    const auto& cache_info = lru_list_.GetNodeByCacheId(cache_id);
    return cache_info.key;
}

CacheId CacheManager::PageCacheToCacheId(const uint8_t* page_cache) {
    const auto diff = page_cache - page_pool_;
    const CacheId cache_id = diff / pager_->page_size();
    return cache_id;
}

} // namespace yudb