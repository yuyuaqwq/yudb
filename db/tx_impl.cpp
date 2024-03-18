#include "yudb/tx_impl.h"

#include "yudb/tx_manager.h"
#include "yudb/db_impl.h"
#include "yudb/bucket_impl.h"
#include "yudb/tx.h"

namespace yudb {

TxImpl::TxImpl(TxManager* tx_manager, const MetaStruct& meta, bool writable, Comparator comparator) :
    tx_manager_{ tx_manager },
    user_bucket_{ this, kUserRootBucketId, &meta_format_.user_root, writable, comparator },
    writable_{ writable }
{
    CopyMetaInfo(&meta_format_, meta);
}

BucketId TxImpl::NewSubBucket(PageId* root_pgid, bool writable, Comparator comparator) {
    BucketId new_bucket_id = sub_bucket_cache_.size();
    sub_bucket_cache_.emplace_back(std::make_unique<BucketImpl>(this, new_bucket_id, root_pgid, writable, comparator));
    return new_bucket_id;
}

BucketImpl& TxImpl::AtSubBucket(BucketId bucket_id) {
    return *sub_bucket_cache_[bucket_id];
}

void TxImpl::DeleteSubBucket(BucketId bucket_id) {
    assert(sub_bucket_cache_[bucket_id].get());
    sub_bucket_cache_[bucket_id].reset();
}

void TxImpl::RollBack() {
    if (writable_) {
        tx_manager_->RollBack();
    } else {
        tx_manager_->RollBack(txid());
    }
}

void TxImpl::Commit() {
    assert(writable_);
    if (user_bucket_.has_sub_bucket_map()) {
        for (auto& iter : user_bucket_.sub_bucket_map()) {
            user_bucket_.Put(iter.first.c_str(), iter.first.size(), &iter.second.second, sizeof(iter.second.second), true);
        }
    }
    for (auto& bucket : sub_bucket_cache_) {
        if (!bucket || !bucket->has_sub_bucket_map()) continue;
        for (auto& iter : bucket->sub_bucket_map()) {
            bucket->Put(iter.first.c_str(), iter.first.size(), &iter.second.second, sizeof(iter.second.second), true);
        }
    }
    tx_manager_->Commit();
}

bool TxImpl::CopyNeeded(TxId txid) const {
    auto current_txid = this->txid();
    // 如果是当前Wal中开启的写事务分配的页面，并且不会有读事务看见，则可以释放
    if (tx_manager_->IsTxExpired(txid) && txid > tx_manager_->persisted_txid()) {
        return false;
    }
    // 如果是当前写事务分配的页面也允许直接释放
    return txid < current_txid;
}

void TxImpl::AppendSubBucketLog(BucketId bucket_id, std::span<const uint8_t> key) {
    tx_manager_->AppendSubBucketLog(bucket_id, key);
}

void TxImpl::AppendPutLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket) {
    tx_manager_->AppendPutLog(bucket_id, key, value, is_bucket);
}

void TxImpl::AppendDeleteLog(BucketId bucket_id, std::span<const uint8_t> key) {
    tx_manager_->AppendDeleteLog(bucket_id, key);
}

Pager& TxImpl::pager() const { return tx_manager_->pager(); }


ViewTx::ViewTx(TxManager* tx_manager, const MetaStruct& meta, std::shared_mutex* mmap_mutex, Comparator comparator) :
    tx_{ tx_manager, meta, false, comparator },
    mmap_lock_{ *mmap_mutex } {}

ViewTx::~ViewTx() {
    tx_.RollBack();
}


ViewBucket ViewTx::UserBucket() {
    auto& root_bucket = tx_.user_bucket();
    return ViewBucket{ &root_bucket };
}

UpdateTx::UpdateTx(TxImpl* tx) : tx_{ tx } {}

UpdateTx::~UpdateTx() {
    if (tx_) {
        RollBack();
    }
}

UpdateBucket UpdateTx::UserBucket() {
    auto& root_bucket = tx_->user_bucket();
    return UpdateBucket{ &root_bucket };
}

void UpdateTx::RollBack() {
    if (tx_ == nullptr) {
        throw TxManagerError{ "invalid tx." };
    }
    tx_->RollBack();
    tx_ = nullptr;
}

void UpdateTx::Commit() {
    if (tx_ == nullptr) {
        throw TxManagerError{ "invalid tx." };
    }
    tx_->Commit();
    tx_ = nullptr;
}

} // yudb