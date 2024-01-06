#include "bucket.h"

#include "txer.h"

namespace yudb {

Bucket::Bucket(Pager* pager, Tx* tx, PageId* btree_root, bool writable) :
    pager_{ pager },
    tx_{ tx },
    btree_{ this, btree_root },
    writable_{ writable } {}


Bucket& Bucket::SubBucket(std::string_view key, bool writable) {
    auto iter = Get(key);
    auto root_pgid = iter->value<PageId>();
    auto map_iter = sub_bucket_map_.find(key.data());
    uint32_t index;
    if (map_iter == sub_bucket_map_.end()) {
        index = tx_->NewSubBucket(&map_iter->second.second, writable);
    }
    else {
        index = map_iter->second.first;
    }
    return tx_->AsSubBucket(index);
}


} // namespace yudb