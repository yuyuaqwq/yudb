#include "yudb/pager.h"

#include "yudb/db_impl.h"
#include "yudb/node.h"

namespace yudb {

Pager::Pager(DBImpl* db, PageSize page_size) : db_{ db },
    page_size_{ page_size },
    tmp_page_{ reinterpret_cast<uint8_t*>(operator new(page_size)) } {}

Pager::~Pager() {
    operator delete(tmp_page_);
}

uint8_t* Pager::GetPtr(PageId pgid, size_t offset) {
    auto ptr = db().db_file_mmap().data();
    return reinterpret_cast<uint8_t*>(ptr + (pgid * page_size_) + offset);
}

void Pager::Write(PageId pgid, const uint8_t* cache, PageCount count) {
    WriteByBytes(pgid, 0, cache, page_size_ * count);
}

void Pager::WriteByBytes(PageId pgid, size_t offset, const uint8_t* buf, size_t bytes) {
    auto dst = GetPtr(pgid, offset);
    std::memcpy(dst, buf, bytes);
}

void Pager::WriteAllDirtyPages() {
    //std::vector<PageCount> sort_arr;
    //sort_arr.reserve(db_->options()->cache_pool_page_count);
    //auto& lru_list = cache_manager_.lru_list();
    //for (auto& iter : lru_list) {
    //    if (iter.value().dirty) {
    //        assert(iter.value().reference_count == 0);
    //        sort_arr.push_back(iter.key());
    //    }
    //}
    //std::sort(sort_arr.begin(), sort_arr.end());
    //for (auto& pgid : sort_arr) {
    //    auto [cache_info, page_cache] = cache_manager_.Reference(pgid, true);
    //    Write(pgid, page_cache, 1);
    //    assert(cache_info->dirty);
    //    cache_info->dirty = false;
    //    cache_manager_.Dereference(page_cache);
    //}
}

void Pager::Rollback() {
    for (auto& alloc_pair : alloc_records_) {
        FreeToMap(alloc_pair.first, alloc_pair.second);
    }
    alloc_records_.clear();
}

PageId Pager::Alloc(PageCount count) {
    auto& update_tx = db_->tx_manager().update_tx();
    PageId pgid = kPageInvalidId;
    for (auto iter = free_map_.begin(); iter != free_map_.end(); ++iter) {
        auto free_count = iter->second;
        assert(free_count > 0);
        if (free_count < count) {
            continue;
        }
        pgid = iter->first;
        free_map_.erase(iter);
        if (count < free_count) {
            free_count -= count;
            auto free_pgid = pgid + count;
            auto [_, success] = free_map_.insert({ free_pgid , free_count });
            assert(success);
        }
        alloc_records_.push_back({ pgid, count });
        break;
    }
#ifndef NDEBUG
    if (pgid != kPageInvalidId) {
        for (PageCount i = 0; i < count; ++i) {
            auto iter = debug_free_set_.find(pgid + i);
            assert(iter != debug_free_set_.end());
            debug_free_set_.erase(iter);
        }
    }
#endif
    
    if (pgid == kPageInvalidId) {
        auto& page_count = update_tx.meta_struct().page_count;
        if (page_count + count < page_count) {
            throw PagerError{ "page allocation failed, there are not enough available pages." };
        }
        pgid = page_count;
        page_count += count;

        size_t min_size = page_count * page_size_;
        auto map_size = db_->db_file_mmap().size();
        if (min_size > map_size) {
            db_->Remmap(min_size);
        }
    }
    return pgid;
}

void Pager::Free(PageId free_pgid, PageCount free_count) {
    assert(free_pgid != kPageInvalidId);
    auto& update_tx = db_->tx_manager().update_tx();
    auto& root_bucket = update_tx.user_bucket();

    auto iter = pending_map_.find(update_tx.txid());
    if (iter == pending_map_.end()) {
        const auto res = pending_map_.insert({ update_tx.txid(), {} });
        iter = res.first;
    }
    iter->second.push_back({ free_pgid, free_count });
}

Page Pager::Copy(const Page& page) {
    auto new_pgid = Alloc(1);
    auto new_page = Reference(new_pgid, true);
    std::memcpy(new_page.page_buf(), page.page_buf(), page_size_);
    Free(page.page_id(), 1);
    return new_page;
}

Page Pager::Copy(PageId pgid) {
    auto page = Reference(pgid, false);
    return Copy(std::move(page));
}

void Pager::Release(TxId releasable_txid) {
    alloc_records_.clear();
    for (auto iter = pending_map_.begin(); iter != pending_map_.end(); ) {
        if (iter->first >= releasable_txid) {
            break;
        }
        for (auto& [free_pgid, free_count] : iter->second) {
            FreeToMap(free_pgid, free_count);
        }
        pending_map_.erase(iter++);
    }
}

void Pager::LoadFreeList() {
    auto& update_tx = db_->tx_manager().update_tx();
    auto& meta = update_tx.meta_struct();
    if (meta.free_list_pgid == kPageInvalidId) {
        return;
    }
    size_t bytes = meta.free_pair_count * sizeof(PagePair);
    auto ptr = GetPtr(meta.free_list_pgid, 0);
    auto free_list = reinterpret_cast<const PagePair*>(ptr);
    for (size_t i = 0; i < meta.free_pair_count; ++i) {
        // SaveFreeList会直接保存未合并的pending pages
        // 这里将其合并到free map
        if (free_list[i].second == 0) {
            continue;
        }
        FreeToMap(free_list[i].first, free_list[i].second);
    }
}

void Pager::SaveFreeList() {
    auto& update_tx = db_->tx_manager().update_tx();
    auto& meta = update_tx.meta_struct();

    // 释放原free_list
    if (meta.free_list_pgid != kPageInvalidId) {
        Free(meta.free_list_pgid, meta.free_list_page_count);
    }

    uint32_t pair_count = free_map_.size();
    for (auto& pending_pair : pending_map_) {
        pair_count += pending_pair.second.size();
    }
    size_t bytes = pair_count * sizeof(PagePair);
    meta.free_list_page_count = bytes / page_size();
    if (bytes % page_size()) ++meta.free_list_page_count;
    meta.free_list_pgid = Alloc(meta.free_list_page_count);

    std::vector<uint8_t> buf(bytes);
    uint32_t i = 0;
    for (auto& pair : free_map_) {
        std::memcpy(&buf[i * sizeof(PagePair)], &pair, sizeof(pair));
        ++i;
    }

    for (auto& pending_pair : pending_map_) {
        std::memcpy(&buf[i * sizeof(PagePair)], &pending_pair.second[0], pending_pair.second.size() * sizeof(PagePair));
        i += pending_pair.second.size();
    }
    meta.free_pair_count = i;

    WriteByBytes(meta.free_list_pgid, 0, buf.data(), meta.free_pair_count * sizeof(PagePair));
}

PageId Pager::GetPageIdByPtr(const uint8_t* page_ptr) const {
    auto ptr = db().db_file_mmap().data();
    const auto diff = page_ptr - reinterpret_cast<const uint8_t*>(ptr);
    const PageId page_id = diff / page_size_;
    return page_id;
}

PageCount Pager::GetPageCount(const size_t bytes) const {
    PageCount page_count = bytes / page_size_;
    if (bytes % page_size_) ++page_count;
    return page_count;
}

Page Pager::Reference(PageId pgid, bool dirty) {
    assert(pgid != kPageInvalidId);
    assert(pgid < kPageMaxCount);
    return Page{ this, GetPtr(pgid, 0) };
}

Page Pager::AddReference(uint8_t* page_buf) {
    return Page{ this, page_buf };
}

void Pager::Dereference(const uint8_t* page_buf) {

}

void Pager::FreeToMap(PageId pgid, PageCount count) {
#ifndef NDEBUG
    for (PageCount i = 0; i < count; ++i) {
        auto [_, success] = debug_free_set_.insert(pgid + i);
        assert(success);
    }
#endif

    const auto next_pgid = pgid + count;
    auto tmp_iter = free_map_.lower_bound(pgid);
    if (tmp_iter != free_map_.end()) {
        if (tmp_iter->first == next_pgid) {
            count += tmp_iter->second;
            free_map_.erase(tmp_iter--);
        }
    } else {
        if (!free_map_.empty()) {
            tmp_iter = --free_map_.end();
        }
    }
    
    if (tmp_iter != free_map_.end() && tmp_iter->first + tmp_iter->second == pgid) {
        tmp_iter->second += count;
    } else {
        free_map_.insert({ pgid, count });
    }
}

} // namespace yudb