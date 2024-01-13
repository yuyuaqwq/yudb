#pragma once

#include <variant>
#include <map>

#include "btree.h"
#include "inline_bucket.h"
#include "bucket_iterator.h"

namespace yudb {

class Pager;
class Tx;

class Bucket : noncopyable {
public:
    using Iterator = BucketIterator;

public:
    Bucket(Pager* pager, Tx* tx, PageId* btree_root, bool writable);

    Bucket(Pager* pager, Tx* tx, std::span<const uint8_t> inline_bucket_data, bool writable);


    Bucket(Bucket&& right) noexcept : 
        pager_{ right.pager_ },
        tx_{ right.tx_ },
        btree_{ std::move(right.btree_) },
        writable_{ right.writable_ },
        sub_bucket_map_{ std::move(right.sub_bucket_map_) },
        inlineable_{ right.inlineable_ },
        max_branch_ele_count_{ right.max_branch_ele_count_ },
        max_leaf_ele_count_{ right.max_leaf_ele_count_ }
    {
        btree_.set_bucket(this);
    }

    void operator=(Bucket&& right) noexcept {
        pager_ = right.pager_;
        tx_ = right.tx_;
        btree_ = std::move(right.btree_);
        btree_.set_bucket(this);
        writable_ = right.writable_;
        sub_bucket_map_ = std::move(right.sub_bucket_map_);
        inlineable_ = right.inlineable_;
        max_branch_ele_count_ = right.max_branch_ele_count_;
        max_leaf_ele_count_ = right.max_leaf_ele_count_;
    }


    Iterator Get(const void* key_buf, size_t key_size) const {
        if (inlineable_) {
            return Iterator{ inline_bucket_.Get(key_buf, key_size) };
        }
        else {
            return Iterator{ btree_.Get({ reinterpret_cast<const uint8_t*>(key_buf), key_size }) };
        }
    }

    Iterator Get(std::string_view key) const {
        return Get(key.data(), key.size());
    }

    Iterator LowerBound(const void* key_buf, size_t key_size) const {
        return Iterator{ btree_.LowerBound({ reinterpret_cast<const uint8_t*>(key_buf), key_size }) };
    }

    Iterator LowerBound(std::string_view key) const {
        return LowerBound(key.data(), key.size());
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

    bool Delete(const void* key_buf, size_t key_size) {
        return btree_.Delete({ reinterpret_cast<const uint8_t*>(key_buf), key_size });
    }


    Bucket& SubBucket(std::string_view key, bool writable);


    Iterator begin() const noexcept {
        return Iterator{ btree_.begin() };
    }

    Iterator end() const noexcept {
        return Iterator{ btree_.end() };
    }



    Pager& pager() const { return *pager_; }

    Tx& tx() { return *tx_; }

    bool writable() const { return writable_; }

    BTree& btree() { return btree_; }

    uint16_t max_leaf_ele_count() { return max_leaf_ele_count_; }

    uint16_t max_branch_ele_count() { return max_branch_ele_count_; }


    void Print(bool str = false) const { btree_.Print(str); }

protected:
    friend class Tx;
    friend class InlineBucket;

    Pager* pager_;
    Tx* tx_;

    bool writable_;

    bool inlineable_;
    BTree btree_;
    std::map<std::string, std::pair<uint32_t, PageId>> sub_bucket_map_;       // PageIdÎªkPageInvalidIdÊ±£¬ÊÇInline Bucket
    InlineBucket inline_bucket_;
    
    uint16_t max_leaf_ele_count_;
    uint16_t max_branch_ele_count_;
    
    
};






} // namespace yudb