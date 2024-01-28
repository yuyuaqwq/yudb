#include "tx.h"

#include "tx_manager.h"
#include "db.h"
#include "bucket.h"

namespace yudb {

Tx::Tx(TxManager* tx_manager, const MetaFormat& meta, bool writable) :
    tx_manager_{ tx_manager },
    root_bucket_{ &pager(), this, &meta_.root, writable },
    writable_{ writable }
{
    TxManager::CopyMeta(&meta_, meta);
}

Tx::Tx(Tx&& right) noexcept :
    tx_manager_{ right.tx_manager_ },
    root_bucket_{ std::move(right.root_bucket_) },
    writable_{ right.writable_ }
{
    TxManager::CopyMeta(&meta_, right.meta_);
}

void Tx::operator=(Tx&& right) noexcept {
    tx_manager_ = right.tx_manager_;
    TxManager::CopyMeta(&meta_, right.meta_);
}

Pager& Tx::pager() { return tx_manager_->pager(); }


void Tx::RollBack() {
    tx_manager_->RollBack();
}

void Tx::RollBack(TxId view_txid) {
    tx_manager_->RollBack(view_txid);
}

void Tx::Commit() {
    for (auto& iter : root_bucket_.sub_bucket_map_) {
        root_bucket_.Put(iter.first.c_str(), iter.first.size(), &iter.second.second, sizeof(iter.second.second));
    }
    for (auto& bucket : sub_bucket_cache_) {
        for (auto& iter : bucket->sub_bucket_map_) {
            bucket->Put(iter.first.c_str(), iter.first.size(), &iter.second.second, sizeof(iter.second.second));
        }
    }
    tx_manager_->Commit();
}

} // yudb