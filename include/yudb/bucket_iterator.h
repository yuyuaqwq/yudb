//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <string>

#include "yudb/btree_iterator.h"

namespace yudb {

class BucketIterator {
public:
    using iterator_category = std::bidirectional_iterator_tag;

    using value_type = BucketIterator;
    using difference_type = std::ptrdiff_t;
    using pointer = BucketIterator*;
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