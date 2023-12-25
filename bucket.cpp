#include "bucket.h"

#include "txer.h"

namespace yudb {

Bucket::Bucket(Pager* pager, Tx* tx, PageId& btree_root) :
    pager_{ pager },
    tx_{ tx },
    btree_{ this, btree_root } {}

} // namespace yudb