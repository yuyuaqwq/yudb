#include "txer.h"

#include "db.h"



namespace yudb {

Txer::Txer(Db* db) :
    db_{ db } {}

UpdateTx Txer::Update() {
    if (!update_tx_) {
        update_tx_ = std::make_unique<Tx>(this, db_->metaer_.meta(), true);
    }
    else {
        std::destroy_at(update_tx_.get());
        std::construct_at(update_tx_.get(), this, db_->metaer_.meta(), true);
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

ViewTx Txer::View() {
    auto iter = view_tx_map_.find(db_->metaer_.meta().txid);
    if (iter == view_tx_map_.end()) {
        view_tx_map_.insert({ db_->metaer_.meta().txid , 1});
    }
    else {
        ++iter->second;
    }
    return ViewTx{ this, db_->metaer_.meta() };
}

void Txer::RollBack() {
    pager().RollbackPending();
}

void Txer::RollBack(TxId view_txid) {
    auto iter = view_tx_map_.find(view_txid);
    assert(iter != view_tx_map_.end());
    assert(iter->second > 0);
    --iter->second;
    if (iter->second == 0) {
        view_tx_map_.erase(iter);
    }
}

void Txer::Commit() {
    pager().CommitPending();
    CopyMeta(&db_->metaer_.meta(), update_tx_->meta_);
}


Pager& Txer::pager() {
    return *db_->pager_;
}


void Txer::CopyMeta(Meta* dst, const Meta& src) {
    dst->root = src.root;
    dst->page_count = src.page_count;
    dst->txid = src.txid;
}

} // namespace yudb