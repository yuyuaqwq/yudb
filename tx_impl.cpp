#include "tx_impl.h"

#include "tx_manager.h"
#include "db_impl.h"
#include "bucket_impl.h"

namespace yudb {

TxImpl::TxImpl(TxManager* tx_manager, const MetaFormat& meta, bool writable) :
    tx_manager_{ tx_manager },
    root_bucket_{ this, kRootBucketId, &meta_format_.root, writable },
    writable_{ writable }
{
    MetaFormatCopy(&meta_format_, meta);
}


BucketId TxImpl::NewSubBucket(PageId* root_pgid, bool writable) {
    BucketId new_bucket_id = sub_bucket_cache_.size();
    sub_bucket_cache_.emplace_back(std::make_unique<BucketImpl>(this, new_bucket_id, root_pgid, writable));
    return new_bucket_id;
}
BucketId TxImpl::NewSubBucket(std::span<const uint8_t> inline_bucket_data, bool writable) {
    BucketId new_bucket_id = sub_bucket_cache_.size();
    sub_bucket_cache_.emplace_back(std::make_unique<BucketImpl>(this, new_bucket_id, inline_bucket_data, writable));
    return new_bucket_id;
}
BucketImpl& TxImpl::AtSubBucket(BucketId bucket_id) {
    return *sub_bucket_cache_[bucket_id];
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
    for (auto& iter : root_bucket_.sub_bucket_map()) {
        root_bucket_.Put(iter.first.c_str(), iter.first.size(), &iter.second.second, sizeof(iter.second.second));
    }
    for (auto& bucket : sub_bucket_cache_) {
        for (auto& iter : bucket->sub_bucket_map()) {
            bucket->Put(iter.first.c_str(), iter.first.size(), &iter.second.second, sizeof(iter.second.second));
        }
    }
    tx_manager_->Commit();
}

bool TxImpl::IsLegacyTx(TxId txid) const {
    return txid < this->txid();
}

void TxImpl::AppendPutLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    tx_manager_->AppendPutLog(bucket_id, key, value);
}
void TxImpl::AppendInsertLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    tx_manager_->AppendInsertLog(bucket_id, key, value);
}
void TxImpl::AppendDeleteLog(BucketId bucket_id, std::span<const uint8_t> key) {
    tx_manager_->AppendDeleteLog(bucket_id, key);
}


Pager& TxImpl::pager() { return tx_manager_->pager(); }


} // yudb