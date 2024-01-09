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
    return UpdateTx{ update_tx_.get() };
}

ViewTx Txer::View() {
    return ViewTx{ this, db_->metaer_.meta() };
}

void Txer::Commit() {
    CopyMeta(&db_->metaer_.meta(), update_tx_->meta_);
}


Pager& Txer::pager() {
    return *db_->pager_;
}


void Txer::CopyMeta(Meta* dst, const Meta& src) {
    dst->root = src.root;
    dst->free_db_root = src.free_db_root;
    dst->page_count = src.page_count;
    dst->txid = src.txid;
}

} // namespace yudb