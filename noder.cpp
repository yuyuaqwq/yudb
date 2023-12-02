#include "noder.h"

#include "btree.h"

namespace yudb {

Noder::Noder(BTree* btree, uint8_t* page) :
    btree_{ btree },
    pager_{ btree_->pager_ },
    node_{ reinterpret_cast<Node*>(page) },
    overflower_{ this, &node_->overflow } {}

} // namespace yudb