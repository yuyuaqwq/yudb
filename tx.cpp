#include "tx.h"

#include "txer.h"
#include "db.h"
#include "bucket.h"

namespace yudb {

Tx::Tx(Txer* txer, const Meta& meta) :
        txer_{ txer }
{
    txer_->CopyMeta(&meta_, meta);
}

Tx::Tx(Tx&& right) noexcept :
    txer_{ right.txer_ }
{
    txer_->CopyMeta(&meta_, right.meta_);
}

void Tx::operator=(Tx&& right) noexcept {
    txer_ = right.txer_;
    txer_->CopyMeta(&meta_, right.meta_);
}


Pager* Tx::pager() { return txer_->pager(); }



ViewTx::ViewTx(Txer* txer, const Meta& meta) :
    Tx{ txer, meta },
    bucket_{ pager(), this, meta_.root } {}

ViewBucket& ViewTx::RootBucket() {
    return bucket_;
}


UpdateTx::UpdateTx(Txer* txer, const Meta& meta) :
    Tx{ txer, meta },
    bucket_{ pager(), this, meta_.root } {}

UpdateBucket& UpdateTx::RootBucket() {
    return bucket_;
}

void UpdateTx::RollBack() {

}

void UpdateTx::Commit() {
    txer_->Commit();
}

} // yudb