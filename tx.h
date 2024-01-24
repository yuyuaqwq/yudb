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

    Bucket& RootBucket() { return root_bucket_; }


    uint32_t NewSubBucket(PageId* root_pgid, bool writable) {
        sub_bucket_cache_.emplace_back(std::make_unique<Bucket>(&pager(), this, root_pgid, writable));
        return sub_bucket_cache_.size() - 1;
    }

    uint32_t NewSubBucket(std::span<const uint8_t> inline_bucket_data, bool writable) {
        sub_bucket_cache_.emplace_back(std::make_unique<Bucket>(&pager(), this, inline_bucket_data, writable));
        return sub_bucket_cache_.size() - 1;
    }

    Bucket& AtSubBucket(uint32_t index) {
        return *sub_bucket_cache_[index];
    }


    bool NeedCopy(TxId txid) {
        return txid < this->txid();
    }

    void RollBack();

    void RollBack(TxId view_txid);

    void Commit();


    TxId txid() { return meta_.txid; }

    Pager& pager();

    Meta& meta() { return meta_; }

protected:
    friend class Txer;

    Txer* txer_;
    Meta meta_;

    bool writable_;

    Bucket root_bucket_;
    std::vector<std::unique_ptr<Bucket>> sub_bucket_cache_;
};

} // namespace yudb