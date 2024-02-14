#pragma once

#include <cstdint>
#include <cassert>

#include <iterator>

#include "node_format.h"

namespace yudb {

class NodeIterator {
public:
    using iterator_category = std::random_access_iterator_tag;

    using difference_type = uint16_t;
    using value_type = NodeIterator;
    using difference_type = uint16_t;
    using pointer = NodeIterator*;
    using reference = NodeIterator&;

public:
    NodeIterator(NodeFormat* node, uint16_t index);
    ~NodeIterator();

    NodeIterator(const NodeIterator& right);
    void operator=(const NodeIterator& right);

    const NodeIterator& operator*() const;
    NodeIterator& operator*();
    NodeIterator& operator--() noexcept;
    NodeIterator& operator++() noexcept;
    NodeIterator operator+(const difference_type n) const;
    NodeIterator operator-(const difference_type n) const;
    difference_type operator-(const NodeIterator& right) const noexcept;
    NodeIterator& operator-=(const difference_type off) noexcept;
    NodeIterator& operator+=(const difference_type off) noexcept;
    bool operator==(const NodeIterator& right) const;

    uint16_t index() { return index_; }
    Cell& key() const;
    Cell& value() const;

private:
    NodeFormat* const node_;
    uint16_t index_;
};

} // namespace yudb
