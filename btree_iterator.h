#pragma once

#include <string>
#include <optional>
#include <variant>

#include "page.h"
#include "stack.h"
#include "noder.h"
#include "page_referencer.h"

namespace yudb {

class BTree;

class BTreeIterator {
public:
    using iterator_category = std::bidirectional_iterator_tag;

    using value_type = typename BTreeIterator;
    using difference_type = typename std::ptrdiff_t;
    using pointer = typename BTreeIterator*;
    using reference = const value_type&;

    using Stack = detail::Stack<std::pair<PageId, uint16_t>, 8>;

    enum class Status {
        kDown,
        kNext,
        kEnd,
    };

    enum class CompResult {
        kInvalid,
        kEq,
        kNe,
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


    Status Top(std::span<const uint8_t> key);

    Status Down(std::span<const uint8_t> key);

    std::pair<PageId, uint16_t>& Front();

    const std::pair<PageId, uint16_t>& Front() const;

    void Pop();

    bool Empty() const;


    void PathCopy();


    CompResult comp_result() const { return comp_result_; }

private:
    std::pair<ImmNoder, uint16_t> LeafImmNoder() const;

    std::pair<MutNoder, uint16_t> LeafMutNoder() const;

    std::tuple<const uint8_t*, size_t, std::optional<PageReferencer>>
    KeyCell() const;

    std::tuple<const uint8_t*, size_t, std::optional<PageReferencer>>
    ValueCell() const;

private:
    const BTree* btree_;
    Stack stack_;       // 索引必定是小于等于搜索时key的节点
    CompResult comp_result_{ CompResult::kInvalid };
};

} // namespace yudb