#pragma once

#include "bucket_impl.h"

namespace yudb {

class ViewBucket : noncopyable {
public:
    using Iterator = BucketIterator;
public:
    explicit ViewBucket(BucketImpl* bucket) : bucket_{ bucket } {};
    ~ViewBucket() = default;

    ViewBucket SubViewBucket(std::string_view key) {
        return ViewBucket{ &bucket_->SubBucket(key, false) };
    }

    Iterator Get(const void* key_buf, size_t key_size) const {
        return bucket_->Get(key_buf, key_size );
    }

    Iterator Get(std::string_view key) const {
        return Get(key.data(), key.size());
    }

    Iterator LowerBound(const void* key_buf, size_t key_size) const {
        return bucket_->LowerBound(key_buf, key_size);
    }

    Iterator LowerBound(std::string_view key) const {
        return LowerBound(key.data(), key.size());
    }


    Iterator begin() const noexcept {
        return bucket_->begin();
    }

    Iterator end() const noexcept {
        return bucket_->end();
    }

    
    void Print(bool str = false) const {
        bucket_->Print(str);
    }


protected:
    BucketImpl* bucket_;
};

class UpdateBucket : public ViewBucket {
public:
    using ViewBucket::ViewBucket;
    UpdateBucket() = delete;
    ~UpdateBucket() = default;

    UpdateBucket SubUpdateBucket(std::string_view key) {
        return UpdateBucket{ &bucket_->SubBucket(key, true) };
    }

    void Insert(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size) {
        bucket_->Insert(key_buf, key_size, value_buf, value_size);
    }

    void Insert(std::string_view key, std::string_view value) {
        Insert(key.data(), key.size(), value.data(), value.size());
    }

    void Put(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size) {
        bucket_->Put(key_buf, key_size, value_buf, value_size);
    }

    void Put(std::string_view key, std::string_view value) {
        Put(key.data(), key.size(), value.data(), value.size());
    }
    
    bool Delete(const void* key_buf, size_t key_size) {
        return bucket_->Delete(key_buf, key_size);
    }

    bool Delete(std::string_view key) {
        return bucket_->Delete(key.data(), key.size());
    }
};

} // namespace yudb