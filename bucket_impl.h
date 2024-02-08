#pragma once

#include <variant>
#include <map>

#include "btree.h"
#include "inline_bucket.h"
#include "bucket_impl_iterator.h"

namespace yudb {

class TxImpl;

class BucketImpl : noncopyable {
public:
    using Iterator = BucketImplIterator;

public:
    BucketImpl(TxImpl* tx, PageId* root_pgid, bool writable);
    BucketImpl(TxImpl* tx, std::span<const uint8_t> inline_bucket_data, bool writable);

    Iterator Get(const void* key_buf, size_t key_size) {
        if (inlineable_) {
            return Iterator{ inline_bucket_.Get(key_buf, key_size) };
        }
        else {
            return Iterator{ btree_.Get({ reinterpret_cast<const uint8_t*>(key_buf), key_size }) };
        }
    }
    Iterator Get(std::string_view key) {
        return Get(key.data(), key.size());
    }
    Iterator LowerBound(const void* key_buf, size_t key_size) {
        return Iterator{ btree_.LowerBound({ reinterpret_cast<const uint8_t*>(key_buf), key_size }) };
    }
    Iterator LowerBound(std::string_view key) {
        return LowerBound(key.data(), key.size());
    }
    void Insert(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size) {
        btree_.Insert(
            { reinterpret_cast<const uint8_t*>(key_buf), key_size },
            { reinterpret_cast<const uint8_t*>(value_buf), value_size }
        );
    }
    void Insert(std::string_view key, std::string_view value) {
        Insert(key.data(), key.size(), value.data(), value.size());
    }
    void Put(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size) {
        btree_.Put(
            { reinterpret_cast<const uint8_t*>(key_buf), key_size },
            { reinterpret_cast<const uint8_t*>(value_buf), value_size }
        );
    }
    void Put(std::string_view key, std::string_view value) {
        Put(key.data(), key.size(), value.data(), value.size());
    }
    void Update(Iterator* iter, const void* value_buf, size_t value_size) {

        btree_.Update(&std::get<BTreeIterator>(iter->iterator_), { reinterpret_cast<const uint8_t*>(value_buf), value_size});
    }
    void Update(Iterator* iter, std::string_view value) {
        Update(iter, value.data(), value.size());
    }
    bool Delete(const void* key_buf, size_t key_size) {
        return btree_.Delete({ reinterpret_cast<const uint8_t*>(key_buf), key_size });
    }
    bool Delete(std::string_view key) {
        Delete(key.data(), key.size());
    }
    void Delete(Iterator* iter) {

        btree_.Delete(&std::get<BTreeIterator>(iter->iterator_));
    }

    BucketImpl& SubBucket(std::string_view key, bool writable);

    Iterator begin() noexcept {
        return Iterator{ btree_.begin() };
    }
    Iterator end() noexcept {
        return Iterator{ btree_.end() };
    }

    void Print(bool str = false) const { btree_.Print(str); }

    const Pager& pager() const;
    Pager& pager();
    auto& tx() const { return *tx_; }
    auto& writable() const { return writable_; }
    auto& btree() const { return btree_; }
    auto& max_leaf_ele_count() const { return max_leaf_ele_count_; }
    auto& max_branch_ele_count() const { return max_branch_ele_count_; }
    auto& sub_bucket_map() const { return sub_bucket_map_; }

protected:
    TxImpl* tx_;

    bool writable_;

    bool inlineable_;
    BTree btree_;
    std::map<std::string, std::pair<uint32_t, PageId>> sub_bucket_map_;       // PageIdÎªkPageInvalidIdÊ±£¬ÊÇInline Bucket
    InlineBucket inline_bucket_;
    
    uint16_t max_leaf_ele_count_;
    uint16_t max_branch_ele_count_;
};

} // namespace yudb