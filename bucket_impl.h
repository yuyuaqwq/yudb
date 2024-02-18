#pragma once

#include <variant>
#include <map>

#include "btree.h"
#include "inline_bucket.h"
#include "bucket_iterator.h"

namespace yudb {

class TxImpl;

class BucketImpl : noncopyable {
public:
    using Iterator = BucketIterator;

public:
    BucketImpl(TxImpl* tx, BucketId bucket_id, PageId* root_pgid, bool writable);
    BucketImpl(TxImpl* tx, BucketId bucket_id, std::span<const uint8_t> inline_bucket_data, bool writable);

    Iterator Get(const void* key_buf, size_t key_size);
    Iterator Get(std::string_view key);
    Iterator LowerBound(const void* key_buf, size_t key_size);
    Iterator LowerBound(std::string_view key);
    void Insert(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size);
    void Insert(std::string_view key, std::string_view value);
    void Put(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size);
    void Put(std::string_view key, std::string_view value);
    void Update(Iterator* iter, const void* value_buf, size_t value_size);
    void Update(Iterator* iter, std::string_view value);
    bool Delete(const void* key_buf, size_t key_size);
    bool Delete(std::string_view key);
    void Delete(Iterator* iter);

    BucketImpl& SubBucket(std::string_view key, bool writable);

    Iterator begin() noexcept;
    Iterator end() noexcept;

    void Print(bool str = false);

    Pager& pager() const;
    auto& tx() const { return *tx_; }
    auto& writable() const { return writable_; }
    auto& btree() { assert(btree_.has_value()); return *btree_; }
    auto& btree() const { assert(btree_.has_value()); return *btree_; }
    auto& sub_bucket_map() const { return sub_bucket_map_; }

protected:
    TxImpl* const tx_;

    BucketId bucket_id_;

    const bool writable_;
    const bool inlineable_;
    std::optional<BTree> btree_;
    std::map<std::string, std::pair<BucketId, PageId>> sub_bucket_map_;       // PageIdÎªkPageInvalidIdÊ±£¬ÊÇInline Bucket
    InlineBucket inline_bucket_;
};

} // namespace yudb