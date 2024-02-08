#include "tx_impl.h"

#include "tx_manager.h"
#include "db_impl.h"
#include "bucket_impl.h"

namespace yudb {

TxImpl::TxImpl(TxManager* tx_manager, const MetaFormat& meta, bool writable) :
    tx_manager_{ tx_manager },
    root_bucket_{ this, &meta_format_.root, writable },
    writable_{ writable }
{
    MetaFormatCopy(&meta_format_, meta);
}


Pager& TxImpl::pager() { return tx_manager_->pager(); }


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

} // yudb