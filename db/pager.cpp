#include "yudb/pager.h"

#include "yudb/db_impl.h"
#include "yudb/node.h"

namespace yudb {



Pager::Pager(DBImpl* db, PageSize page_size) : db_{ db },
    page_size_{ page_size },
    cache_manager_{ this },
    tmp_page_{ reinterpret_cast<uint8_t*>(operator new(page_size)) } {}

Pager::~Pager() {
    operator delete(tmp_page_);
}


void Pager::Read(PageId pgid, uint8_t* cache, PageCount count) {
    ReadByBytes(pgid, 0, cache, page_size_);
}

void Pager::ReadByBytes(PageId pgid, size_t offset, uint8_t* cache, size_t bytes) {
    db_->file().Seek(pgid * page_size() + offset, File::PointerMode::kDbFilePointerSet);
    const auto read_size = db_->file().Read(cache + offset, bytes);
    assert(read_size == 0 || read_size == bytes);
}

void Pager::Write(PageId pgid, const uint8_t* cache, PageCount count) {
    WriteByBytes(pgid, 0, cache, page_size_ * count);
}

void Pager::WriteByBytes(PageId pgid, size_t offset, const uint8_t* cache, size_t bytes) {
    db_->file().Seek(pgid * page_size() + offset, File::PointerMode::kDbFilePointerSet);
    db_->file().Write(cache, bytes);
}

void Pager::WriteAllDirtyPages() {
    std::vector<PageCount> sort_arr;
    sort_arr.reserve(db_->options()->cache_pool_page_count);
    auto& lru_list = cache_manager_.lru_list();
    for (auto& iter : lru_list) {
        if (iter.value().dirty) {
            assert(iter.value().reference_count == 0);
            sort_arr.push_back(iter.key());
        }
    }
    std::sort(sort_arr.begin(), sort_arr.end());
    for (auto& pgid : sort_arr) {
        auto [cache_info, page_cache] = cache_manager_.Reference(pgid);
        Write(pgid, page_cache, 1);
        assert(cache_info->dirty);
        cache_info->dirty = false;
        cache_manager_.Dereference(page_cache);
    }
}

PageId Pager::Alloc(PageCount count) {
    auto& update_tx = db_->tx_manager().update_tx();
    auto& root_bucket = update_tx.root_bucket();

    PageId pgid = kPageInvalidId;
    if (update_tx.meta_format().root != kPageInvalidId && free_db_lock_ == false) {
        free_db_lock_ = true;
        auto& free_db = root_bucket.SubBucket(kFreeDBKey, true, UInt32Comparator);
        for (auto& iter : free_db) {
            auto free_count = iter.value<uint32_t>();
            assert(free_count > 0);
            if (free_count < count) {
                continue;
            }
            pgid = iter.key<PageId>();
            auto copy_iter = iter;
            free_db.Delete(&copy_iter);
            if (count < free_count) {
                free_count -= count;
                auto free_pgid = pgid + count;
                free_db.Put(&free_pgid, sizeof(free_pgid), &free_count, sizeof(free_count));
            }
            break;
        }

#ifndef NDEBUG
        if (pgid != kPageInvalidId) {
            for (auto i = 0; i < count; ++i) {
                auto iter = free_page_.find(pgid + i);
                assert(iter != free_page_.end());
                free_page_.erase(iter);
            }
        }
#endif

        free_db_lock_ = false;
    }
    if (pgid == kPageInvalidId) {
        auto& page_count = update_tx.meta_format().page_count;
        if (page_count + count < page_count) {
            throw PagerError{ "page allocation failed, there are not enough available pages." };
        }
        pgid = page_count;
        page_count += count;
    }
    return pgid;
}

void Pager::Free(PageId free_pgid, PageCount free_count) {
    auto& update_tx = db_->tx_manager().update_tx();
    auto& root_bucket = update_tx.root_bucket();

    auto iter = pending_.find(update_tx.txid());
    if (iter == pending_.end()) {
        const auto res = pending_.insert({ update_tx.txid(), {} });
        iter = res.first;
    }
    iter->second.push_back({ free_pgid, free_count });
    assert(free_pgid != kPageInvalidId);
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

Page Pager::Reference(PageId pgid, bool dirty) {
    assert(pgid != kPageInvalidId);
    auto [cache_info, page_cache] = cache_manager_.Reference(pgid);
    if (cache_info->dirty == false && dirty == true) {
        cache_info->dirty = true;
    }
    return Page{ this, page_cache };
}

Page Pager::AddReference(uint8_t* page_cache) {
    cache_manager_.AddReference(page_cache);
    return Page{ this, page_cache };
}

void Pager::Dereference(const uint8_t* page_cache) {
    cache_manager_.Dereference(page_cache);
}

PageId Pager::GetPageIdByCache(const uint8_t* page_cache) {
    return cache_manager_.GetPageIdByCache(page_cache);
}


void Pager::RollbackPending() {
    const auto& update_tx = db_->tx_manager().update_tx();
    pending_.erase(update_tx.txid());
}

void Pager::CommitPending() {
    auto& update_tx = db_->tx_manager().update_tx();
    auto& root = update_tx.root_bucket();
    auto& pending_db = root.SubBucket(kPendingDBKey, true, UInt64Comparator);
    const auto txid = update_tx.txid();
    const auto iter = pending_.find(txid);
    if (iter != pending_.end()) {
        pending_db.Put(&txid, sizeof(txid), &iter->second[0], sizeof(iter->second[0]) * iter->second.size());
        //for (auto& page_iter : iter->second) {
            //pending_db.Put(&page_iter.first, sizeof(page_iter.first), &page_iter.second, sizeof(page_iter.second));
        //}
    }
}

void Pager::FreePending(TxId min_view_txid) {
    auto& update_tx = db_->tx_manager().update_tx();
    auto& root = update_tx.root_bucket();
    if (min_view_txid == kTxInvalidId) {
        // 上一个写事务pending的页面还不能释放，因为当前写事务进行时，其他线程可能会开启新的读事务
        min_view_txid = update_tx.txid() - 1;
        assert(min_view_txid != kTxInvalidId);
    }

    for (auto iter = pending_.begin(); iter != pending_.end(); ) {
        if (iter->first >= min_view_txid) {
            break;
        } 
        for (auto& [free_pgid, free_count] : iter->second) {
            FreeToFreeDB(free_pgid, free_count);
        }
        pending_.erase(iter++);
    }
}

void Pager::ClearPending() {
    auto& update_tx = db_->tx_manager().update_tx();
    auto& root = update_tx.root_bucket();
    auto& pending_db = root.SubBucket(kPendingDBKey, true, UInt64Comparator);

    //std::vector<PageId> pending_list;
    std::vector<TxId> pending_list;
    for (auto& iter : pending_db) {
        //auto pgid = iter.key<PageId>();
        //auto count = iter.value<PageCount>();
        //FreeToFreeDB(pgid, count);
        //pending_list.push_back(pgid);

        auto pair_arr_buf = iter.key();
        auto pair_arr = reinterpret_cast<const std::pair<PageId, PageCount>*>(pair_arr_buf.data());
        auto size = pair_arr_buf.size() / sizeof(pair_arr[0]);
        for (size_t i = 0; i < size; ++i) {
            FreeToFreeDB(pair_arr[i].first, pair_arr[i].second);
        }
    }

    for (auto& key : pending_list) {
        pending_db.Delete(&key, sizeof(key));
    }
}

void Pager::FreeToFreeDB(PageId pgid, PageCount count) {
#ifndef NDEBUG
    for (auto i = 0; i < count; ++i) {
        auto [_, success] = free_page_.insert(pgid + i);
        assert(success);
    }
#endif

    free_db_lock_ = true;
    auto& update_tx = db_->tx_manager().update_tx();
    auto& root = update_tx.root_bucket();
    auto& free_db = root.SubBucket(kFreeDBKey, true, UInt32Comparator);
    
    assert(free_db.Get(&pgid, sizeof(pgid)) == free_db.end());

    const auto next_pgid = pgid + count;
    auto next_iter = free_db.Get(&next_pgid, sizeof(next_pgid));
    if (next_iter != free_db.end()) {
        count += next_iter.value<PageCount>();
        free_db.Delete(&next_iter);
    }
    const auto prev_pgid = pgid - count;
    auto prev_iter = free_db.Get(&next_pgid, sizeof(next_pgid));
    if (prev_iter != free_db.end()) {
        count += prev_iter.value<PageCount>();
        free_db.Update(&prev_iter, &count, sizeof(count));
    } else {
        free_db.Put(&pgid, sizeof(pgid), &count, sizeof(count));
    }
    free_db_lock_ = false;
}

} // namespace yudb