#pragma once

#include <cstdint>
#include <memory>
#include <string_view>

#include "noncopyable.h"
#include "txid.h"
#include "meta.h"
#include "bucket.h"
#include "bucket_public.h"

namespace yudb {

class Txer;

class Tx : noncopyable {
public:
    Tx(Txer* txer, const Meta& meta, bool writable);

    Tx(Tx&& right) noexcept;
    void operator=(Tx&& right) noexcept;

    Bucket& RootBucket() { return root_; }


    uint32_t NewSubBucket(PageId* root_pgid, bool writable) {
        sub_bucket_cache_.emplace_back(Bucket{ &pager(), this, root_pgid, writable });
        return sub_bucket_cache_.size() - 1;
    }

    Bucket& AsSubBucket(uint32_t index) {
        return sub_bucket_cache_[index];
    }

    bool IsExpiredTxId(TxId txid) {
        return txid < this->txid();
    }

    void Commit();


    TxId txid() { return meta_.txid; }

    Pager& pager();

    Meta& meta() { return meta_; }



protected:
    friend class Txer;

    Txer* txer_;
    Meta meta_;
    bool writable_;
    Bucket root_;
    std::vector<Bucket> sub_bucket_cache_;
};




} // namespace yudb