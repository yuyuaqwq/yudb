#pragma once

#include <unordered_map>

#include "btree.h"

namespace yudb {

class Pager;
class Tx;

class Bucket : noncopyable {
public:
    using Iterator = BTreeIterator;

public:
    Bucket(Pager* pager, Tx* tx, PageId* btree_root, bool writable);

    Bucket(Bucket&& right) noexcept : 
        pager_{ right.pager_ },
        tx_{ right.tx_ },
        btree_{ std::move(right.btree_) },
        writable_{ right.writable_ },
        sub_bucket_map_{ std::move(right.sub_bucket_map_) } {}

    void operator=(Bucket&& right) noexcept {
        pager_ = right.pager_;
        tx_ = right.tx_;
        btree_ = std::move(right.btree_);
        writable_ = right.writable_;
        sub_bucket_map_ = std::move(right.sub_bucket_map_);
    }


    Iterator Get(const void* key_buf, size_t key_size) const {
        return btree_.Get({ reinterpret_cast<const uint8_t*>(key_buf), key_size });
    }

    Iterator Get(std::string_view key) const {
        return Get(key.data(), key.size());
    }

    Iterator LowerBound(const void* key_buf, size_t key_size) const {
        return btree_.LowerBound({ reinterpret_cast<const uint8_t*>(key_buf), key_size });
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
        return btree_.begin();
    }

    Iterator end() const noexcept {
        return btree_.end();
    }



    Pager& pager() { return *pager_; }

    bool writable() const { return writable_; }

    BTree& btree() { return btree_; }

    Tx& tx() { return *tx_; }

    void Print(bool str = false) const {
        btree_.Print(str);
     }

protected:
    Pager* pager_;
    Tx* tx_;
    BTree btree_;
    bool writable_;

    std::unordered_map<std::string, std::pair<uint32_t, PageId>> sub_bucket_map_;
};






} // namespace yudb