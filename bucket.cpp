#include "bucket.h"

#include "txer.h"

namespace yudb {

Bucket::Bucket(Pager* pager, Tx* tx, PageId& btree_root) :
    tx_{ tx },
    btree_{ std::make_unique<BTree>(pager, tx, btree_root) } {}

} // namespace yudb