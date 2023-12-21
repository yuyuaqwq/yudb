#include "txer.h"

#include "db.h"

namespace yudb {

UpdateTx Txer::Update() {
    UpdateTx tx{ this, db_->metaer_.meta() };
    ++tx.meta_.tx_id;
    return tx;
}

ViewTx Txer::View() {
    ViewTx tx{ this, db_->metaer_.meta() };
    ++tx.meta_.tx_id;
    return tx;
}


} // namespace yudb