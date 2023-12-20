#include "btree.h"

#include "tx.h"

namespace yudb {

void BTree::PathPageCopy(Iterator* iter) {
    auto new_pgid = kPageInvalidId;

    auto& [pgid, index] = iter->Cur();
    Noder noder{ this, pgid };
    if (noder.node()->last_write_txid == tx_->tx_id()) {
        return;
    }

    for (ptrdiff_t i = iter->Size() - 1; i >= 0; i--) {
        auto& [pgid, index] = iter->Index(i);
        Noder noder{ this, pgid };

        if (new_pgid != kPageInvalidId) {
            noder.BranchSetLeftChild(index, new_pgid);
        }

        new_pgid = pager_->Alloc(1);
        Noder new_noder{ this, new_pgid };

        memcpy(new_noder.page_cache(), noder.page_cache(), pager_->page_size());

        new_noder.node()->last_write_txid = tx_->tx_id();

        // Pending
        pager_->Free(pgid, 1);

        pgid = new_pgid;
    }

    root_pgid_ = new_pgid;
}


} // namespace yudb