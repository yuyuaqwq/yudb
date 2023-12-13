#include "noder.h"

#include "btree.h"

namespace yudb {

Noder::Noder(BTree* btree, PageId page_id) :
    btree_{ btree },
    pager_{ btree_->pager_ },
    page_ref_{ pager_->Reference(page_id) },
    node_{ reinterpret_cast<Node*>(page_ref_.page_cache()) },
    overflower_{ this, &node_->overflow } {}

} // namespace yudb