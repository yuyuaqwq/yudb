#include "bucket.h"

#include "txer.h"

namespace yudb {

ViewBucket::ViewBucket(Pager* pager, Tx* tx, PageId& btree_root) :
    tx_{ tx },
    btree_{ std::make_unique<BTree>(this, btree_root) } {}



void UpdateBucket::PathCopy(Iterator* iter) {
    auto new_pgid = kPageInvalidId;
    auto& [pgid, index] = iter->Cur();
    Noder noder{ btree_.get(), pgid };

    for (ptrdiff_t i = iter->Size() - 1; i >= 0; i--) {
        if (noder.node()->last_write_txid == tx_->tx_id()) {
            return;
        }

        auto& [pgid, index] = iter->Index(i);
        Noder noder{ btree_.get(), pgid };

        if (new_pgid != kPageInvalidId) {
            noder.BranchSetLeftChild(index, new_pgid);
        }

        new_pgid = pager_->Alloc(1);
        Noder new_noder{ btree_.get(), new_pgid };

        memcpy(new_noder.page_cache(), noder.page_cache(), pager_->page_size());

        new_noder.node()->last_write_txid = tx_->tx_id();

        // Pending
        pager_->Free(pgid, 1);

        pgid = new_pgid;
    }
    iter->btree_->root_pgid_  = new_pgid;
}


} // namespace yudb