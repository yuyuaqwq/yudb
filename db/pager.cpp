#include "db/pager.h"

#include "db/db_impl.h"
#include "db/node.h"

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
    db_->file().Seek(pgid * page_size() + offset);
    const auto read_size = db_->file().Read(cache + offset, bytes);
    assert(read_size == 0 || read_size == bytes);
}

void Pager::Write(PageId pgid, const uint8_t* cache, PageCount count) {
    WriteByBytes(pgid, 0, cache, page_size_ * count);
}

void Pager::WriteByBytes(PageId pgid, size_t offset, const uint8_t* cache, size_t bytes) {
    db_->file().Seek(pgid * page_size() + offset);
    db_->file().Write(cache, bytes);
}

void Pager::SyncAllPage() {
    std::vector<PageCount> sort_arr;
    sort_arr.reserve(db_->options()->cache_page_pool_count);
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
    auto& update_tx = db_->tx_manager().CurrentUpdateTx();
    auto& root_bucket = update_tx.RootBucket();

    PageId pgid = kPageInvalidId;
    if (update_tx.meta_format().root != kPageInvalidId) {
        auto& free_bucket = root_bucket.SubBucket("fr_pg", true);
        for (auto iter : free_bucket) {
            auto free_count = iter.value<uint32_t>();
            assert(free_count > 0);
            if (free_count < count) {
                continue;
            }
            pgid = iter.key<PageId>();
            auto copy_iter = iter;
            free_bucket.Delete(&copy_iter);
            if (count < free_count) {
                free_count -= count;
                auto free_pgid = pgid + count;
                free_bucket.Put(&free_pgid, sizeof(free_pgid), &free_count, sizeof(free_count));
            }
            break;
        }
    }
    if (pgid == kPageInvalidId) {
        pgid = update_tx.meta_format().page_count;
        auto new_pgid = pgid += count;
        if (new_pgid < pgid) {
            throw std::runtime_error("page allocation failed, there are not enough available pages.");
        }
        update_tx.meta_format().page_count += count;
    }
    return pgid;
}

void Pager::Free(PageId free_pgid, PageCount free_count) {
    auto& update_tx = db_->tx_manager().CurrentUpdateTx();
    auto& root_bucket = update_tx.RootBucket();

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
    const auto& update_tx = db_->tx_manager().CurrentUpdateTx();
    pending_.erase(update_tx.txid());
}

void Pager::CommitPending() {
    return;// 暂时注释，处理可变长kv
    auto& update_tx = db_->tx_manager().CurrentUpdateTx();
    auto& root_bucket = update_tx.RootBucket();
    auto& pending_bucket = root_bucket.SubBucket("tx_pd", true);
    const auto txid = update_tx.txid();
    const auto iter = pending_.find(txid);
    if (iter != pending_.end()) {
        pending_bucket.Put(&txid, sizeof(txid), &iter->second[0], iter->second.size() * sizeof(iter->second[0]));
    }
}

void Pager::ClearPending(TxId min_view_txid) {
    auto& update_tx = db_->tx_manager().CurrentUpdateTx();
    auto& root_bucket = update_tx.RootBucket();
    auto& free_bucket = root_bucket.SubBucket("fr_pg", true);
    auto& pending_bucket = root_bucket.SubBucket("tx_pd", true);
    // 如果是崩溃后重启，需要从pending_bucket中free

    if(min_view_txid == kTxInvalidId) {
        // 不能全部释放，上一个写事务pending的还不能释放，因为当前写事务进行时，其他线程可能会开启新的读事务
        min_view_txid = update_tx.txid() - 1;
        assert(min_view_txid != kTxInvalidId);
    }
    for (auto iter = pending_.begin(); iter != pending_.end(); ) {
        if (iter->first >= min_view_txid) {
            break;
        } 
        for (auto& [free_pgid, free_count] : iter->second) {
            //printf("free:%d\n", free_pgid);
            assert(free_bucket.Get(&free_pgid, free_pgid) == free_bucket.end());
            const auto next_pgid = free_pgid + free_count;
            auto next_iter = free_bucket.Get(&next_pgid, sizeof(next_pgid));
            if (next_iter != free_bucket.end()) {
                free_count += next_iter.value<PageCount>();
                free_bucket.Delete(&next_iter);
            }
            bool insert = true;
            if (next_iter != free_bucket.begin()) {
                auto prev_iter = next_iter--;
                const auto prev_pgid = prev_iter.key<PageId>();
                if (prev_pgid == free_pgid - free_count) {
                    insert = false;
                    free_count += prev_iter.value<PageCount>();
                    free_bucket.Update(&prev_iter, &free_count, sizeof(free_count));
                }
            }
            if (insert) {
                free_bucket.Insert(&free_pgid, sizeof(free_pgid), &free_count, sizeof(free_count));
            }
        }
        pending_.erase(iter++);
    }
}

} // namespace yudb