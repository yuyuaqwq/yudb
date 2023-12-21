#include "tx.h"

#include "txer.h"
#include "db.h"
#include "bucket.h"

namespace yudb {

Pager* Tx::pager() { return txer_->db_->pager_.get(); }


ViewBucket& ViewTx::RootBucket() {
    return bucket_;
}


UpdateBucket& UpdateTx::RootBucket() {
    return bucket_;
}


} // yudb