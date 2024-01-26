#pragma once

#include <string>
#include <optional>
#include <variant>

#include "page.h"
#include "stack.h"
#include "node_operator.h"
#include "page_reference.h"

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
    BTreeIterator(const BTree* btree) : btree_{ btree } {}


    reference operator*() const noexcept;

    const BTreeIterator* operator->() const noexcept;

    BTreeIterator& operator++() noexcept;

    BTreeIterator operator++(int) noexcept;

    BTreeIterator& operator--() noexcept;

    BTreeIterator operator--(int) noexcept;

    bool operator==(const BTreeIterator& right) const noexcept;

    
    bool is_bucket() const;

    void set_is_bucket();

    bool is_inline_bucket() const;

    void set_is_inline_bucket();


    template <class KeyT>
    KeyT key() const {
        auto [buf, size, ref] = KeyCell();
        if (size != sizeof(KeyT)) {
            throw std::runtime_error("The size of the key does not match.");
        }
        KeyT key;
        std::memcpy(&key, buf, size);
        return key;
    }

    template <class ValueT>
    ValueT value() const {
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



    void First(PageId pgid);

    void Last(PageId pgid);

    void Next();

    void Prev();


    bool Top(std::span<const uint8_t> key);

    bool Down(std::span<const uint8_t> key);

    std::pair<PageId, uint16_t>& Front();

    const std::pair<PageId, uint16_t>& Front() const;

    void Pop();

    bool Empty() const;


    void PathCopy();


    Status status() const { return status_; }

private:
    std::pair<ImmNodeOperator, uint16_t> LeafImmNodeOperator() const;

    std::pair<MutNodeOperator, uint16_t> LeafMutNodeOperator() const;

    std::tuple<const uint8_t*, size_t, std::optional<PageReference>>
    KeyCell() const;

    std::tuple<const uint8_t*, size_t, std::optional<PageReference>>
    ValueCell() const;

private:
    const BTree* btree_;
    Stack stack_;       // 索引必定是小于等于搜索时key的节点
    Status status_{ Status::kInvalid };
};

} // namespace yudb