#include "txer.h"

#include "db.h"

namespace yudb {

Tx Txer::Begin() {
    Tx tx{ this, db_->metaer_.meta() };
    ++tx.meta_.tx_id;
    return tx;
}


} // namespace yudb