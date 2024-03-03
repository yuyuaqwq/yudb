#pragma once

#include <string>
#include <optional>
#include <variant>

#include "db/page_format.h"
#include "db/node.h"
#include "util/stack.h"

namespace yudb {

class BTree;

/*
* ջ��/kInvalid��ʾend
*/
class BTreeIterator {
public:
    using iterator_category = std::bidirectional_iterator_tag;

    using value_type = typename BTreeIterator;
    using difference_type = typename std::ptrdiff_t;
    using pointer = typename BTreeIterator*;
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
            throw std::runtime_error("The size of the key does not match.");
        }
        KeyT key;
        std::memcpy(&key, span.data(), sizeof(KeyT));
        return key;
    }
    template <class ValueT> ValueT value() const {
        auto span = GetValue();
        if (span.size() != sizeof(ValueT)) {
            throw std::runtime_error("The size of the value does not match.");
        }
        ValueT value;
        std::memcpy(&value, span.data(), sizeof(ValueT));
        return value;
    }
    std::string_view key() const;
    std::string_view value() const;
    bool is_bucket() const;
    void set_is_bucket();
    //bool is_inline_bucket() const;
    //void set_is_inline_bucket();
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
    BTree* const btree_;
    Stack stack_;       // �����ض���С�ڵ�������ʱkey�Ľڵ�
    Status status_{ Status::kInvalid };

    mutable std::optional<Node> cached_node_;
};

} // namespace yudb