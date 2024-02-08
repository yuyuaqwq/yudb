#pragma once

#include <cstdint>
#include <memory>
#include <string_view>

#include "noncopyable.h"
#include "tx_format.h"
#include "meta.h"
#include "bucket_impl.h"
#include "bucket.h"

namespace yudb {

class TxManager;

class TxImpl : noncopyable {
public:
    TxImpl(TxManager* tx_manager, const MetaFormat& meta, bool writable);
    ~TxImpl() = default;

    BucketImpl& RootBucket() { return root_bucket_; }
    const BucketImpl& RootBucket() const { return root_bucket_; }
    uint32_t NewSubBucket(PageId* root_pgid, bool writable);
    uint32_t NewSubBucket(std::span<const uint8_t> inline_bucket_data, bool writable);
    BucketImpl& AtSubBucket(uint32_t index);

    void RollBack();
    void Commit();

    bool IsLegacyTx(TxId txid) const;

    auto& txid() const { return meta_format_.txid; }
    void set_txid(TxId txid) { meta_format_.txid = txid; }
    Pager& pager();
    auto& meta_format() const { return meta_format_; }
    auto& meta_format() { return meta_format_; }

protected:
    TxManager* const tx_manager_;
    MetaFormat meta_format_;

    const bool writable_;

    BucketImpl root_bucket_;
    std::vector<std::unique_ptr<BucketImpl>> sub_bucket_cache_;
};

} // namespace yudb