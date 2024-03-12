#pragma once

#include "yudb/bucket_impl.h"
#include "yudb/bucket_iterator.h"
#include "yudb/noncopyable.h"

namespace yudb {

class BucketImpl;

class ViewBucket : noncopyable {
public:
    using Iterator = BucketIterator;

public:
    explicit ViewBucket(BucketImpl* bucket);
    ~ViewBucket();

    ViewBucket SubViewBucket(std::string_view key, const Comparator comparator);
    ViewBucket SubViewBucket(std::string_view key);
    Iterator Get(const void* key_buf, size_t key_size) const;
    Iterator Get(std::string_view key) const;
    Iterator LowerBound(const void* key_buf, size_t key_size) const;
    Iterator LowerBound(std::string_view key) const;

    Iterator begin() const noexcept;
    Iterator end() const noexcept;

    // void Print(bool str = false) const;

protected:
    BucketImpl* const bucket_;
};

class UpdateBucket : public ViewBucket {
public:
    using ViewBucket::ViewBucket;

    UpdateBucket() = delete;
    ~UpdateBucket();

    UpdateBucket SubUpdateBucket(std::string_view key, const Comparator comparator);
    UpdateBucket SubUpdateBucket(std::string_view key);
    bool DeleteSubBucket(std::string_view key);

    void Put(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size);
    void Put(std::string_view key, std::string_view value);
    bool Delete(const void* key_buf, size_t key_size);
    bool Delete(std::string_view key);
    
};

} // namespace yudb