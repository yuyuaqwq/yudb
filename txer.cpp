#include "txer.h"

#include "db.h"

namespace yudb {

Txer::Txer(Db* db) :
    db_{ db }{}

UpdateTx& Txer::Update() {
    if (!update_tx_) {
        update_tx_ = std::make_unique<UpdateTx>(this, db_->metaer_.meta());
        db_->pager_->set_update_tx(update_tx_.get());
    }
    //else {
    //    CopyMeta(&update_tx_->meta_, db_->metaer_.meta());
    //}
    ++update_tx_->meta_.txid;
    return *update_tx_;
}

ViewTx Txer::View() {
    ViewTx tx{ this, db_->metaer_.meta() };
    ++tx.meta_.txid;
    return tx;
}


void Txer::CopyMeta(Meta* dst, const Meta& src) {
    dst->root = src.root;
    dst->page_count = src.page_count;
    dst->txid = src.txid;
}

void Txer::Commit() {
    CopyMeta(&db_->metaer_.meta(), update_tx_->meta_);
}


} // namespace yudb