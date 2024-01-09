#include "tx.h"

#include "txer.h"
#include "db.h"
#include "bucket.h"

namespace yudb {

Tx::Tx(Txer* txer, const Meta& meta, bool writable) :
    txer_{ txer },
    root_{ &pager(), this, &meta_.root, writable },
    writable_{ writable }
{
    Txer::CopyMeta(&meta_, meta);
}

Tx::Tx(Tx&& right) noexcept :
    txer_{ right.txer_ },
    root_{ std::move(right.root_) },
    writable_{ right.writable_ }
{
    Txer::CopyMeta(&meta_, right.meta_);
}

void Tx::operator=(Tx&& right) noexcept {
    txer_ = right.txer_;
    Txer::CopyMeta(&meta_, right.meta_);
}

Pager& Tx::pager() { return txer_->pager(); }


void Tx::Commit() {
    txer_->Commit();
}



} // yudb