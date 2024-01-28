#include "tx_manager.h"

#include "db.h"



namespace yudb {

TxManager::TxManager(DB* db) :
    db_{ db } {}

UpdateTx TxManager::Update() {
    if (!update_tx_) {
        update_tx_ = std::make_unique<Tx>(this, db_->meta_.meta_format(), true);
    }
    else {
        std::destroy_at(update_tx_.get());
        std::construct_at(update_tx_.get(), this, db_->meta_.meta_format(), true);
    }
    ++update_tx_->meta_.txid;
    if (update_tx_->meta_.txid == kInvalidTxId) {
        throw std::runtime_error("TxId overflow.");
    }
    auto iter = view_tx_map_.begin();
    if (iter != view_tx_map_.end()) {
        pager().ClearPending(iter->first);
    }
    else {
        pager().ClearPending(kInvalidTxId);
    }
    return UpdateTx{ update_tx_.get() };
}

ViewTx TxManager::View() {
    auto iter = view_tx_map_.find(db_->meta_.meta_format().txid);
    if (iter == view_tx_map_.end()) {
        view_tx_map_.insert({ db_->meta_.meta_format().txid , 1});
    }
    else {
        ++iter->second;
    }
    return ViewTx{ this, db_->meta_.meta_format() };
}

void TxManager::RollBack() {
    pager().RollbackPending();
}

void TxManager::RollBack(TxId view_txid) {
    auto iter = view_tx_map_.find(view_txid);
    assert(iter != view_tx_map_.end());
    assert(iter->second > 0);
    --iter->second;
    if (iter->second == 0) {
        view_tx_map_.erase(iter);
    }
}

void TxManager::Commit() {
    pager().CommitPending();
    CopyMeta(&db_->meta_.meta_format(), update_tx_->meta_);
}


Pager& TxManager::pager() {
    return *db_->pager_;
}


void TxManager::CopyMeta(MetaFormat* dst, const MetaFormat& src) {
    dst->root = src.root;
    dst->page_count = src.page_count;
    dst->txid = src.txid;
}

} // namespace yudb