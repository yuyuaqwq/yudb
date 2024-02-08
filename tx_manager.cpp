#include "tx_manager.h"

#include "db_impl.h"


namespace yudb {

TxManager::TxManager(DBImpl* db) :
    db_{ db } {}

UpdateTx TxManager::Update() {
    update_tx_.emplace(this, db_->meta().meta_format(), true);
    update_tx_->set_txid(update_tx_->txid() + 1);
    if (update_tx_->txid() == kInvalidTxId) {
        throw std::runtime_error("txid overflow.");
    }
    const auto iter = view_tx_map_.cbegin();
    if (iter != view_tx_map_.end()) {
        pager().ClearPending(iter->first);
    } else {
        pager().ClearPending(kInvalidTxId);
    }
    return UpdateTx{ &*update_tx_ };
}

ViewTx TxManager::View() {
    const auto iter = view_tx_map_.find(db_->meta().meta_format().txid);
    if (iter == view_tx_map_.end()) {
        view_tx_map_.insert({ db_->meta().meta_format().txid , 1});
    } else {
        ++iter->second;
    }
    return ViewTx{ this, db_->meta().meta_format()};
}

void TxManager::RollBack() {
    pager().RollbackPending();
    update_tx_ = std::nullopt;
}

void TxManager::RollBack(TxId view_txid) {
    const auto iter = view_tx_map_.find(view_txid);
    assert(iter != view_tx_map_.end());
    assert(iter->second > 0);
    --iter->second;
    if (iter->second == 0) {
        view_tx_map_.erase(iter);
    }
}

void TxManager::Commit() {
    pager().CommitPending();
    MetaFormatCopy(&db_->meta().meta_format(), update_tx_->meta_format());

    pager().SyncAllPage();
    db_->meta().Save();
    db_->meta().Switch();

    update_tx_ = std::nullopt;
}

const Pager& TxManager::pager() const {
    return db_->pager();
}

Pager& TxManager::pager() {
    return db_->pager();
}

} // namespace yudb