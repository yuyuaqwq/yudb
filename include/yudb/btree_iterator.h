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
#include <optional>
#include <variant>

#include <yudb/page_format.h>
#include <yudb/node.h>
#include <yudb/stack.h>
#include <yudb/error.h>

namespace yudb {

class BTree;

// B+Tree Iterator
// Stack empty/kInvalid represents end
class BTreeIterator {
public:
    using iterator_category = std::bidirectional_iterator_tag;

    using value_type = BTreeIterator;
    using difference_type = std::ptrdiff_t;
    using pointer = BTreeIterator*;
    using reference = const value_type&;

    using Stack = detail::Stack<std::pair<PageId, SlotId>, 32>;

    enum class Status {
        kInvalid,
        kEq,
        kNe,
        kIter,
    };

public:
    explicit BTreeIterator(BTree* btree);
    BTreeIterator(const BTreeIterator& right);
    void operator=(const BTreeIterator& right);

    reference operator*() const noexcept;
    const BTreeIterator* operator->() const noexcept;
    BTreeIterator& operator++() noexcept;
    BTreeIterator operator++(int) noexcept;
    BTreeIterator& operator--() noexcept;
    BTreeIterator operator--(int) noexcept;
    bool operator==(const BTreeIterator& right) const noexcept;

    template <class KeyT> KeyT key() const {
        auto span = GetKey();
        if (span.size() != sizeof(KeyT)) {
            throw InvalidArgumentError("The size of the key does not match.");
        }
        KeyT key;
        std::memcpy(&key, span.data(), sizeof(KeyT));
        return key;
    }
    template <class ValueT> ValueT value() const {
        auto span = GetValue();
        if (span.size() != sizeof(ValueT)) {
            throw InvalidArgumentError("The size of the value does not match.");
        }
        ValueT value;
        std::memcpy(&value, span.data(), sizeof(ValueT));
        return value;
    }
    std::string_view key() const;
    std::string_view value() const;
    bool is_bucket() const;
    Status status() const { return status_; }

    void First(PageId pgid);
    void Last(PageId pgid);
    void Next();
    void Prev();

    bool Empty() const;
    bool Top(std::span<const uint8_t> key);
    bool Down(std::span<const uint8_t> key);
    std::pair<PageId, SlotId>& Front();
    const std::pair<PageId, SlotId>& Front() const;
    void Push(const std::pair<PageId, SlotId>& v);
    void Pop();

    void CopyAllPagesByPath();

private:
    std::pair<LeafNode&, SlotId> GetLeafNode(bool dirty) const;
    std::span<const uint8_t> GetKey() const;
    std::span<const uint8_t> GetValue() const;

private:
    BTree* btree_;
    Stack stack_;       // The index must be a node that is less than or equal to the key at the time of search.
    Status status_ = Status::kInvalid;

    mutable std::optional<Node> cached_node_;
};

} // namespace yudb
