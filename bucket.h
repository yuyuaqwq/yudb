#pragma once

#include "btree.h"

namespace yudb {

class Tx;
class UpdateTx;
class Pager;
class ViewBucket;

class Bucket {
public:
    using Iterator = BTreeIterator;

public:
    Bucket(Pager* pager, Tx* tx, PageId& btree_root);

    ViewBucket& SubViewBucket(std::string_view key) const {
        auto iter = Get(key);
        auto root_pgid = iter->value<PageId>();
        //return ViewBucket{ pager_, tx_, root_pgid };
    }

    Iterator Get(const void* key_buf, size_t key_size) const {
        return btree_.Get({ reinterpret_cast<const uint8_t*>(key_buf), key_size });
    }

    Iterator Get(std::string_view key) const {
        return Get(key.data(), key.size());
    }


    Iterator begin() const noexcept {
        return btree_.begin();
    }

    Iterator end() const noexcept {
        return btree_.end();
    }


    Pager* pager() const { return pager_; }

    Tx* tx() const { return tx_; }

    UpdateTx* update_tx() const { return reinterpret_cast<UpdateTx*>(tx_); }


    void Print(bool str = false) const {
        btree_.Print(str);
    }

protected:
    Pager* pager_;
    Tx* tx_;
    BTree btree_;
};

class ViewBucket : public Bucket {
public:
    using Bucket::Bucket;
};

class UpdateBucket : public Bucket {
public:
    using Bucket::Bucket;

    UpdateBucket& SubUpdateBucket(std::string_view key) {
        auto iter = Get(key);
        auto root_pgid = iter->value<PageId>();
        //return UpdateBucket{ pager_, tx_, root_pgid};
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

private:
    friend class Pager;
};

static_assert(sizeof(Bucket) == sizeof(ViewBucket) && sizeof(Bucket) == sizeof(UpdateBucket), "bucket size mismatch.");

} // namespace yudb