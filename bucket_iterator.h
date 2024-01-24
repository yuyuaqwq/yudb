#pragma once

#include <string>

#include "btree_iterator.h"
#include "inline_bucket_iterator.h"

namespace yudb {

class BucketIterator {
public:
    using iterator_category = std::bidirectional_iterator_tag;

    using value_type = typename BucketIterator;
    using difference_type = typename std::ptrdiff_t;
    using pointer = typename BucketIterator*;
    using reference = const value_type&;

    enum class ValueType {
        kBucket,
        kInlineBucket,
        kData,
    };

public:
    explicit BucketIterator(const BTreeIterator& iterator_) : iterator_{ iterator_ } {}
    explicit BucketIterator(const InlineBucketIterator& iterator_) : iterator_{ iterator_ } {}

public:
    reference operator*() const noexcept {
        return *this;
    }

    const BucketIterator* operator->() const noexcept {
        return this;
    }

    BucketIterator& operator++() noexcept {
        if (iterator_.index() == kInline) {
            auto& iter = std::get<kInline>(iterator_);
            ++iter;
        }
        else {
            auto& iter = std::get<kBTree>(iterator_);
            ++iter;
        }
        return *this;
    }

    BucketIterator operator++(int) noexcept {
        BucketIterator tmp = *this;
        ++tmp;
        return tmp;
    }

    BucketIterator& operator--() noexcept {
        if (iterator_.index() == kInline) {
            auto& iter = std::get<kInline>(iterator_);
            --iter;
        }
        else {
            auto& iter = std::get<kBTree>(iterator_);
            --iter;
        }
        return *this;
    }

    BucketIterator operator--(int) noexcept {
        BucketIterator tmp = *this;
        --tmp;
        return tmp;
    }

    bool operator==(const BucketIterator& right) const noexcept {
        if (iterator_.index() == kInline) {
            auto& iter1 = std::get<kInline>(iterator_);
            auto& iter2 = std::get<kInline>(right.iterator_);
            return iter1 == iter2;
        }
        else {
            auto& iter1 = std::get<kBTree>(iterator_);
            auto& iter2 = std::get<kBTree>(right.iterator_);
            return iter1 == iter2;
        }
    }


    template <class KeyT>
    KeyT key() const {
        if (iterator_.index() == kInline) {
            auto& iter = std::get<kInline>(iterator_);
            KeyT res;
            std::memcpy(&res, iter->first.data(), sizeof(KeyT));
            return res;
        }
        else {
            auto& iter = std::get<kBTree>(iterator_);
            return iter->key<KeyT>();
        }
    }

    template <class ValueT>
    ValueT value() const {
        if (iterator_.index() == kInline) {
            auto& iter = std::get<kInline>(iterator_);
            ValueT res;
            std::memcpy(&res, iter->first.data(), sizeof(ValueT));
            return res;
        }
        else {
            auto& iter = std::get<kBTree>(iterator_);
            return iter->value<ValueT>();
        }
    }

    std::string key() const {
        if (iterator_.index() == kInline) {
            auto& iter = std::get<kInline>(iterator_);
            return iter->first;
        }
        else {
            auto& iter = std::get<kBTree>(iterator_);
            return iter->key();
        }
    }

    std::string value() const {
        if (iterator_.index() == kInline) {
            auto& iter = std::get<kInline>(iterator_);
            return iter->second;
        }
        else {
            auto& iter = std::get<kBTree>(iterator_);
            return iter->value();
        }
    }

    bool is_bucket() const {
        if (iterator_.index() == kInline) {

        }
        else {
            auto& iter = std::get<kBTree>(iterator_);
            return iter.is_bucket();
        }
        return false;
    }

private:

    void set_is_bucket() {
        if (iterator_.index() == kInline) {

        }
        else {
            auto& iter = std::get<kBTree>(iterator_);
            iter.set_is_bucket();
        }
    }

    bool is_inline_bucket() const {
        if (iterator_.index() == kInline) {

        }
        else {
            auto& iter = std::get<kBTree>(iterator_);
            return iter.is_inline_bucket();
        }
        return false;
    }

    void set_is_inline_bucket() {
        if (iterator_.index() == kInline) {

        }
        else {
            auto& iter = std::get<kBTree>(iterator_);
            iter.set_is_inline_bucket();
        }
    }


private:
    friend class Bucket;

    enum IteratorType{
        kInline = 0,
        kBTree = 1,
    };

    std::variant<InlineBucketIterator, BTreeIterator> iterator_;
};


} // namespace yudb