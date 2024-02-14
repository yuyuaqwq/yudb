#pragma once

#include <string>
#include <optional>
#include <variant>

#include "page_format.h"
#include "stack.h"
#include "node.h"

namespace yudb {

class BTree;

/*
* 栈空/kInvalid表示end
*/
class BTreeIterator {
public:
    using iterator_category = std::bidirectional_iterator_tag;

    using value_type = typename BTreeIterator;
    using difference_type = typename std::ptrdiff_t;
    using pointer = typename BTreeIterator*;
    using reference = const value_type&;

    using Stack = detail::Stack<std::pair<PageId, uint16_t>, 8>;

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
        auto [buf, size, ref] = KeyCell();
        if (size != sizeof(KeyT)) {
            throw std::runtime_error("The size of the key does not match.");
        }
        KeyT key;
        std::memcpy(&key, buf, size);
        return key;
    }
    template <class ValueT> ValueT value() const {
        auto [buf, size, ref] = ValueCell();
        if (size != sizeof(ValueT)) {
            throw std::runtime_error("The size of the value does not match.");
        }
        ValueT value;
        std::memcpy(&value, buf, size);
        return value;
    }
    std::string key() const;
    std::string value() const;
    bool is_bucket() const;
    void set_is_bucket();
    bool is_inline_bucket() const;
    void set_is_inline_bucket();
    Status status() const { return status_; }

    void First(PageId pgid);
    void Last(PageId pgid);
    void Next();
    void Prev();

    bool Empty() const;
    bool Top(std::span<const uint8_t> key);
    bool Down(std::span<const uint8_t> key);
    std::pair<PageId, uint16_t>& Front();
    const std::pair<PageId, uint16_t>& Front() const;
    void Pop();

    void PathCopy();
private:
    std::pair<ConstNode, uint16_t> LeafConstNode() const;
    std::pair<Node, uint16_t> LeafNode() const;

    std::tuple<const uint8_t*, uint32_t, std::optional<std::variant<ConstPage, std::string>>>
    KeyCell() const;
    std::tuple<const uint8_t*, uint32_t, std::optional<std::variant<ConstPage, std::string>>>
    ValueCell() const;

private:
    BTree* const btree_;
    Stack stack_;       // 索引必定是小于等于搜索时key的节点
    Status status_{ Status::kInvalid };
};

} // namespace yudb