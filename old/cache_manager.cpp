#include "yudb/cache_manager.h"

#include "yudb/crc32.h"
#include "yudb/error.h"
#include "yudb/db_impl.h"
#include "yudb/pager.h"

namespace yudb {

CacheManager::CacheManager(Pager* pager) :
    pager_ { pager },
    lru_list_{ pager_->db().options()->cache_pool_page_count }
{
    for (auto& val : fast_map_) {
        val.first = kPageInvalidId;
    }
    page_pool_ = reinterpret_cast<uint8_t*>(operator new(pager_->db().options()->cache_pool_page_count * pager->page_size()));
}

CacheManager::~CacheManager() {
    if (page_pool_) {
        operator delete(page_pool_);
        page_pool_ = nullptr;
    }
}

std::pair<CacheInfo*, uint8_t*> CacheManager::Reference(PageId pgid, bool dirty) {
    auto fast_map_index = pgid % kCacheFastMapPoolCount;
    if (fast_map_[fast_map_index].first == pgid) {
        const auto cache_id = fast_map_[fast_map_index].second;
        auto& node = lru_list_.GetNodeByCacheId(cache_id);
        auto cache_info = &node.value;
        ++cache_info->reference_count;
        lru_list_.set_front(cache_id);
        return { cache_info, &page_pool_[cache_id * pager_->page_size()] };
    } else {
        fast_map_[fast_map_index].first = pgid;
    }

    auto [cache_info, cache_id] = lru_list_.get(pgid);
    if (!cache_info) {
        if (lru_list_.full()) {
            // ÈôÐèÒªÌÔÌ­Ò³ÃæÐ´»Ø´ÅÅÌ
            auto iter = lru_list_.rbegin();
            if (iter.value().reference_count != 0) {
                do {
                    --iter;
                    if (iter == lru_list_.rend()) {
                        throw CacheManagerError{ "unable to reallocate cache." };
                    }
                    if (iter.value().reference_count == 0) {
                        break;
                    }
                } while (true);
            }
            auto evict_pgid = iter.key();
            auto evict_cache_id = iter.id();
            auto evict_cache_info = iter.value();
            lru_list_.erase(iter.key());

            auto evict_fast_map_index = evict_pgid % kCacheFastMapPoolCount;
            if (fast_map_[evict_fast_map_index].first == evict_pgid) {
                fast_map_[evict_fast_map_index].first = kPageInvalidId;
            }

            // printf("evict:%d(%d) ", evict_pgid, evict_cache_info.dirty);
            if (evict_cache_info.dirty) {
#ifndef NDEBUG
                {
                    Crc32 crc32;
                    crc32.Append(&page_pool_[evict_cache_id * pager_->page_size()], pager_->page_size());
                    debug_page_crc32_map_[evict_pgid] = crc32.End();
                }
#endif
                pager_->Write(evict_pgid, &page_pool_[evict_cache_id * pager_->page_size()], 1);

            }
        }

        auto evict = lru_list_.push_front(pgid, CacheInfo{0});
        assert(!evict);

        const auto pair = lru_list_.get(pgid);
        cache_info = pair.first;
        cache_info->dirty = false;
        cache_id = pair.second;
        auto cache = &page_pool_[cache_id * pager_->page_size()];
        pager_->Read(pgid, cache, 1);
#ifndef NDEBUG
        {
            auto iter = debug_page_crc32_map_.find(pgid);
            if (iter != debug_page_crc32_map_.end()) {
                Crc32 crc32;
                crc32.Append(cache, pager_->page_size());
                auto crc32_res = crc32.End();
                assert(iter->second == crc32_res);
            }
        }
#endif
    }
    fast_map_[fast_map_index].second = cache_id;
    ++cache_info->reference_count;
    if (cache_info->dirty == false && dirty == true) {
        cache_info->dirty = true;
    }
    return { cache_info, &page_pool_[cache_id * pager_->page_size()] };
}

void CacheManager::AddReference(const uint8_t* page_cache) {
    auto cache_id = GetCacheIdByCache(page_cache);
    auto& cache_info = lru_list_.GetNodeByCacheId(cache_id).value;
    ++cache_info.reference_count;
}

void CacheManager::Dereference(const uint8_t* page_cache) {
    auto cache_id = GetCacheIdByCache(page_cache);
    auto& cache_info = lru_list_.GetNodeByCacheId(cache_id).value;
    assert(cache_info.reference_count > 0);
    --cache_info.reference_count;
}

PageId CacheManager::GetPageIdByCache(const uint8_t* page_cache) {
    auto cache_id = GetCacheIdByCache(page_cache);
    const auto& cache_info = lru_list_.GetNodeByCacheId(cache_id);
    return cache_info.key;
}

CacheId CacheManager::GetCacheIdByCache(const uint8_t* page_cache) {
    const auto diff = page_cache - page_pool_;
    const CacheId cache_id = diff / pager_->page_size();
    return cache_id;
}

} // namespace yudb