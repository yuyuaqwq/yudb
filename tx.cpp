#include "tx.h"

#include "txer.h"
#include "db.h"
#include "bucket.h"

namespace yudb {

Tx::Tx(Txer* txer, const Meta& meta, bool writable) :
    txer_{ txer },
    root_bucket_{ &pager(), this, &meta_.root, writable },
    writable_{ writable }
{
    Txer::CopyMeta(&meta_, meta);
}

Tx::Tx(Tx&& right) noexcept :
    txer_{ right.txer_ },
    root_bucket_{ std::move(right.root_bucket_) },
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
    for (auto& iter : root_bucket_.sub_bucket_map_) {
        root_bucket_.Put(iter.first.c_str(), iter.first.size(), &iter.second.second, sizeof(iter.second.second));
    }
    for (auto& bucket : sub_bucket_cache_) {
        for (auto& iter : bucket.sub_bucket_map_) {
            bucket.Put(iter.first.c_str(), iter.first.size(), &iter.second.second, sizeof(iter.second.second));
        }
    }
    txer_->Commit();
}



} // yudb