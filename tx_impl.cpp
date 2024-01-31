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

TxImpl::TxImpl(TxImpl&& right) noexcept :
    tx_manager_{ nullptr },
    root_bucket_{ std::move(right.root_bucket_) },
    writable_{ right.writable_ }
{
    root_bucket_.set_tx(this);
    root_bucket_.set_root_pgid(&meta_format_.root);
    MetaFormatCopy(&meta_format_, right.meta_format_);
}

void TxImpl::operator=(TxImpl&& right) noexcept {
    tx_manager_ = nullptr;
    root_bucket_ = std::move(right.root_bucket_);
    writable_ = right.writable_;
    root_bucket_.set_tx(this);
    root_bucket_.set_root_pgid(&meta_format_.root);
    MetaFormatCopy(&meta_format_, right.meta_format_);
}

Pager& TxImpl::pager() { return tx_manager_->pager(); }


void TxImpl::RollBack() {
    tx_manager_->RollBack();
}

void TxImpl::RollBack(TxId view_txid) {
    tx_manager_->RollBack(view_txid);
}

void TxImpl::Commit() {
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