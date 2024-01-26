#include "pager.h"

#include "db.h"
#include "node_operator.h"
#include "tx.h"

namespace yudb {

void Pager::Read(PageId pgid, void* cache, PageCount count) {
    db_->file_.Seek(pgid * page_size());
    auto read_size = db_->file_.Read(cache, count * page_size());
    assert(read_size == 0 || read_size == count * page_size());
    if (read_size == 0) {
        memset(cache, 0, count * page_size());
    }
}

void Pager::Write(PageId pgid, void* cache, PageCount count) {
    db_->file_.Seek(pgid * page_size());
    db_->file_.Write(cache, count * page_size());
}


PageId Pager::Alloc(PageCount count) {
    auto& update_tx = db_->txer_.CurrentUpdateTx();
    auto& root_bucket = update_tx.RootBucket();

    PageId pgid = kPageInvalidId;
    if (update_tx.meta().root != kPageInvalidId) {
        auto& free_bucket = root_bucket.SubBucket("fr_pg", true);
        for (auto& iter : free_bucket) {
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
        pgid = update_tx.meta().page_count;
        update_tx.meta().page_count += count;
    }
    auto [cache_info, page_cache] = cacher_.Reference(pgid);
    cache_info->dirty = true;

    MutNodeOperator node_operator{ &update_tx.RootBucket().btree(), pgid };
    node_operator.node().last_modified_txid = update_tx.txid();

    cacher_.Dereference(page_cache);
    printf("alloc:%d\n", pgid);
    return pgid;
}

void Pager::Free(PageId free_pgid, PageCount free_count) {
    printf("pending:%d\n", free_pgid);
    auto& update_tx = db_->txer_.CurrentUpdateTx();
    auto& root_bucket = update_tx.RootBucket();

    auto iter = pending_.find(update_tx.txid());
    if (iter == pending_.end()) {
        auto res = pending_.insert({ update_tx.txid(), {} });
        iter = res.first;
    }
    iter->second.push_back({ free_pgid, free_count });
    assert(free_pgid != kPageInvalidId);
}

void Pager::RollbackPending() {
    auto& update_tx = db_->txer_.CurrentUpdateTx();
    pending_.erase(update_tx.txid());
}

void Pager::CommitPending() {
    return;// 暂时注释，处理可变长kv
    auto& update_tx = db_->txer_.CurrentUpdateTx();
    auto& root_bucket = update_tx.RootBucket();
    auto& pending_bucket = root_bucket.SubBucket("tx_pd", true);
    auto txid = update_tx.txid();
    auto iter = pending_.find(txid);
    if (iter != pending_.end()) {
        pending_bucket.Put(&txid, sizeof(txid), &iter->second[0], iter->second.size() * sizeof(iter->second[0]));
    }
}

void Pager::ClearPending(TxId min_view_txid) {
    auto& update_tx = db_->txer_.CurrentUpdateTx();
    auto& root_bucket = update_tx.RootBucket();
    auto& free_bucket = root_bucket.SubBucket("fr_pg", true);
    auto& pending_bucket = root_bucket.SubBucket("tx_pd", true);
    // 如果是崩溃后重启，需要从pending_bucket中free

    if(min_view_txid == kInvalidTxId) {
        // 不能全部释放，上一个写事务pending的还不能释放，因为当前写事务进行时，其他线程可能会开启新的读事务
        min_view_txid = update_tx.txid() - 1;
        assert(min_view_txid != kInvalidTxId);
    }
    for (auto iter = pending_.begin(); iter != pending_.end(); ) {
        if (iter->first >= min_view_txid) {
            break;
        } 
        for (auto& [free_pgid, free_count] : iter->second) {
            printf("free:%d\n", free_pgid);
            assert(free_bucket.Get(&free_pgid, free_pgid) == free_bucket.end());
            auto next_pgid = free_pgid + free_count;
            auto next_iter = free_bucket.Get(&next_pgid, sizeof(next_pgid));
            if (next_iter != free_bucket.end()) {
                free_count += next_iter.value<PageCount>();
                free_bucket.Delete(&next_iter);
            }
            bool insert = true;
            if (next_iter != free_bucket.begin()) {
                auto prev_iter = next_iter--;
                auto prev_pgid = prev_iter.key<PageId>();
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