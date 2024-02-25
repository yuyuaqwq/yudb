#pragma once

#include <cstdint>

#include <array>
#include <memory>
#include <string_view>

#include "noncopyable.h"
#include "tx_format.h"
#include "meta.h"
#include "bucket_impl.h"
#include "bucket.h"

#include "tx_log_format.h"

namespace yudb {

class TxManager;

class TxImpl : noncopyable {
public:
    TxImpl(TxManager* tx_manager, const MetaStruct& meta, bool writable);
    ~TxImpl() = default;

    BucketImpl& RootBucket() { return root_bucket_; }
    const BucketImpl& RootBucket() const { return root_bucket_; }
    BucketId NewSubBucket(PageId* root_pgid, bool writable);
    BucketImpl& AtSubBucket(BucketId bucket_id);

    void RollBack();
    void Commit();

    bool IsLegacyTx(TxId txid) const;

    void AppendPutLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value);
    void AppendInsertLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value);
    void AppendDeleteLog(BucketId bucket_id, std::span<const uint8_t> key);

    auto& txid() const { return meta_format_.txid; }
    void set_txid(TxId txid) { meta_format_.txid = txid; }
    Pager& pager() const;
    auto& tx_manager() const { return *tx_manager_; }
    auto& meta_format() const { return meta_format_; }
    auto& meta_format() { return meta_format_; }

protected:
    TxManager* const tx_manager_;
    MetaStruct meta_format_;

    const bool writable_;

    BucketImpl root_bucket_;
    std::vector<std::unique_ptr<BucketImpl>> sub_bucket_cache_;
};

} // namespace yudb