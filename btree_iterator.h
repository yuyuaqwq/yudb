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

private:
    enum class Status {
        kDown,
        kNext,
        kEnd,
    };

    enum class CompResult {
        kInvalid,
        kEq,
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


    template <class KeyT>
    KeyT key() const {
        auto [buf, size, ref] = KeySpan();
        if (size != sizeof(KeyT)) {
            throw std::runtime_error("The size of the key does not match.");
        }
        KeyT key;
        std::memcpy(&key, buf, size);
        return key;
    }

    template <class ValueT>
    ValueT value() const {
        auto [buf, size, ref] = ValueSpan();
        if (size != sizeof(ValueT)) {
            throw std::runtime_error("The size of the value does not match.");
        }
        ValueT value;
        std::memcpy(&value, buf, size);
        return value;
    }

    std::string key() const;

    std::string value() const;


    CompResult comp_result() const { return comp_result_; }

private:
    void First(PageId pgid);

    void Last(PageId pgid);

    void Next();

    void Prev();

private:
    std::tuple<const uint8_t*, size_t, std::optional<std::variant<PageReferencer, std::vector<uint8_t>>>>
    KeySpan() const;

    std::tuple<const uint8_t*, size_t, std::optional<std::variant<PageReferencer, std::vector<uint8_t>>>>
    ValueSpan() const;

private:
    Status Top(std::span<const uint8_t> key);

    Status Down(std::span<const uint8_t> key);

    std::pair<PageId, uint16_t>& Cur();

    const std::pair<PageId, uint16_t>& Cur() const;

    void Pop();

    bool Empty() const;


private:
    std::pair<PageId, uint16_t>& Index(ptrdiff_t i) { return stack_.index(i); }

    size_t Size() const { return stack_.cur_pos(); }

private:
    friend class BTree;

    const BTree* btree_;
    detail::Stack<std::pair<PageId, uint16_t>, 8> stack_;       // 索引必定是小于等于搜索时key的节点
    CompResult comp_result_{ CompResult::kInvalid };
};

} // namespace yudb