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

    TxImpl(TxImpl&& right) noexcept;
    void operator=(TxImpl&& right) noexcept;

    const auto& txid() const { return meta_format_.txid; }
    void set_txid(TxId txid) { meta_format_.txid = txid; }
    void set_tx_manager(TxManager* tx_manager) { tx_manager_ = tx_manager; }
    Pager& pager();
    const auto& meta_format() const { return meta_format_; }
    auto& meta_format() { return meta_format_; }

    BucketImpl& RootBucket() { return root_bucket_; }
    uint32_t NewSubBucket(PageId* root_pgid, bool writable) {
        sub_bucket_cache_.emplace_back(std::make_unique<BucketImpl>(this, root_pgid, writable));
        return sub_bucket_cache_.size() - 1;
    }
    uint32_t NewSubBucket(std::span<const uint8_t> inline_bucket_data, bool writable) {
        sub_bucket_cache_.emplace_back(std::make_unique<BucketImpl>(this, inline_bucket_data, writable));
        return sub_bucket_cache_.size() - 1;
    }
    BucketImpl& AtSubBucket(uint32_t index) {
        return *sub_bucket_cache_[index];
    }

    void RollBack();
    void RollBack(TxId view_txid);
    void Commit();

    bool IsLegacyTx(TxId txid) const {
        return txid < this->txid();
    }

protected:
    TxManager* tx_manager_;
    MetaFormat meta_format_;

    bool writable_;

    BucketImpl root_bucket_;
    std::vector<std::unique_ptr<BucketImpl>> sub_bucket_cache_;
};

} // namespace yudb