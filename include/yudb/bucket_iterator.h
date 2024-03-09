#pragma once

#include <string>

#include "yudb/btree_iterator.h"

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
    explicit BucketIterator(const BTreeIterator& iterator_) : iter_{ iterator_ } {}

    reference operator*() const noexcept {
        return *this;
    }

    const BucketIterator* operator->() const noexcept {
        return this;
    }

    BucketIterator& operator++() noexcept {
        ++iter_;
        return *this;
    }

    BucketIterator operator++(int) noexcept {
        BucketIterator tmp = *this;
        ++tmp;
        return tmp;
    }

    BucketIterator& operator--() noexcept {
        --iter_;
        return *this;
    }

    BucketIterator operator--(int) noexcept {
        BucketIterator tmp = *this;
        --tmp;
        return tmp;
    }

    bool operator==(const BucketIterator& right) const noexcept {
        return iter_ == right.iter_;
    }

    template <class KeyT>
    KeyT key() const {
        return iter_.key<KeyT>();
    }

    template <class ValueT>
    ValueT value() const {
        return iter_.value<ValueT>();
    }

    std::string_view key() const {
        return iter_.key();
    }

    std::string_view value() const {
        return iter_.value();
    }

    bool is_bucket() const {
        return iter_.is_bucket();
    }

private:
    friend class BucketImpl;

    enum IteratorType{
        kInline = 0,
        kBTree = 1,
    };

    BTreeIterator iter_;
};


} // namespace yudb