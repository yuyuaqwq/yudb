#pragma once

#include <cstdint>
#include <cassert>

#include <iterator>

#include "node.h"


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
    NodeIterator(NodeFormat* node, uint16_t index) : node_{ node }, index_{ index } {}
    ~NodeIterator() = default;

    const NodeIterator& operator*() const { return *this; }
    NodeIterator& operator*() { return *this; }
    NodeIterator& operator--() noexcept {
        --index_;
        return *this;
    }
    NodeIterator& operator++() noexcept {
        ++index_;
        return *this;
    }
    NodeIterator operator+(const difference_type n) const {
        return NodeIterator{ node_, uint16_t(index_ + n) };
    }
    NodeIterator operator-(const difference_type n) const {
        return NodeIterator{ node_, uint16_t(index_ - n) };
    }
    difference_type operator-(const NodeIterator& right) const noexcept {
        return index_ - right.index_;
    }
    NodeIterator& operator-=(const difference_type off) noexcept {
        index_ -= off;
        return *this;
    }
    NodeIterator& operator+=(const difference_type off) noexcept {
        index_ += off;
        return *this;
    }
    bool operator==(const NodeIterator& right) const {
        return node_ == right.node_ && index_ == right.index_;
    }

    uint16_t index() { return index_; }
    Cell& key() const {
        if (node_->type == NodeFormat::Type::kBranch) {
            return node_->body.branch[index_].key;
        }
        else {
            assert(node_->type == NodeFormat::Type::kLeaf);
            return node_->body.leaf[index_].key;
        }
    }
    Cell& value() const {
        assert(node_->type == NodeFormat::Type::kLeaf);
        return node_->body.leaf[index_].value;
    }

private:
    NodeFormat* node_;
    uint16_t index_;
};

} // namespace yudb
