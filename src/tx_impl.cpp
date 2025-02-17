//The MIT License(MIT)
//Copyright ?? 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the ��Software��), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include <atomkv/tx_impl.h>

#include <atomkv/bucket_impl.h>
#include <atomkv/tx.h>

#include "db_impl.h"
#include "tx_manager.h"

namespace atomkv {

TxImpl::TxImpl(TxManager* tx_manager, const MetaStruct& meta, bool writable)
    : tx_manager_(tx_manager)
    , user_bucket_(this, kUserRootBucketId, &meta_format_.user_root, writable, tx_manager_->db().options()->comparator)
    , writable_(writable)
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
    // If the page is allocated by the write transaction currently open in the Wal, and will not be seen by any read transactions, it can be freed
    if (tx_manager_->IsTxExpired(txid) && txid > tx_manager_->persisted_txid()) {
        return false;
    }
    // If the page is allocated by the current write transaction, it can also be directly freed
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


ViewTx::ViewTx(TxManager* tx_manager, const MetaStruct& meta, std::shared_mutex* mmap_mutex) :
    tx_(tx_manager, meta, false),
    mmap_lock_(*mmap_mutex) {}

ViewTx::~ViewTx() {
    tx_.RollBack();
}


ViewBucket ViewTx::UserBucket() {
    auto& root_bucket = tx_.user_bucket();
    return ViewBucket(&root_bucket);
}

UpdateTx::UpdateTx(TxImpl* tx) : tx_{ tx } {}

UpdateTx::~UpdateTx() {
    if (tx_) {
        RollBack();
    }
}

UpdateTx::UpdateTx(UpdateTx&& right) noexcept {
    assert(tx_ == nullptr);
    tx_ = right.tx_;
    right.tx_ = nullptr;
}

UpdateBucket UpdateTx::UserBucket() {
    auto& root_bucket = tx_->user_bucket();
    return UpdateBucket(&root_bucket);
}

void UpdateTx::RollBack() {
    if (tx_ == nullptr) {
        throw std::runtime_error("Invalid tx.");
    }
    tx_->RollBack();
    tx_ = nullptr;
}

void UpdateTx::Commit() {
    if (tx_ == nullptr) {
        throw std::runtime_error("Invalid tx.");
    }
    tx_->Commit();
    tx_ = nullptr;
}

} // atomkv