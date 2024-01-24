#pragma once

#include <cstdint>
#include <cassert>

#include <iterator>

#include "node.h"


namespace yudb {

class NoderIterator {
public:
    using iterator_category = std::random_access_iterator_tag;

    using difference_type = uint16_t;
    using value_type = NoderIterator;
    using difference_type = uint16_t;
    using pointer = NoderIterator*;
    using reference = NoderIterator&;

public:
    NoderIterator(Node* node, uint16_t index) : node_{ node }, index_{ index } {}

    const NoderIterator& operator*() const {
        return *this;
    }

    NoderIterator& operator*() {
        return *this;
    }

    NoderIterator& operator--() noexcept {
        --index_;
        return *this;
    }

    NoderIterator& operator++() noexcept {
        ++index_;
        return *this;
    }

    NoderIterator operator+(const difference_type n) const {
        return NoderIterator{ node_, uint16_t(index_ + n) };
    }

    NoderIterator operator-(const difference_type n) const {
        return NoderIterator{ node_, uint16_t(index_ - n) };
    }

    difference_type operator-(const NoderIterator& right) const noexcept {
        return index_ - right.index_;
    }

    NoderIterator& operator-=(const difference_type off) noexcept {
        index_ -= off;
        return *this;
    }

    NoderIterator& operator+=(const difference_type off) noexcept {
        index_ += off;
        return *this;
    }

    bool operator==(const NoderIterator& right) const {
        return node_ == right.node_ && index_ == right.index_;
    }

    uint16_t index() { return index_; }

    Cell& key() const {
        if (node_->type == Node::Type::kBranch) {
            return node_->body.branch[index_].key;
        }
        else {
            assert(node_->type == Node::Type::kLeaf);
            return node_->body.leaf[index_].key;
        }
    }

    Cell& value() const {
        assert(node_->type == Node::Type::kLeaf);
        return node_->body.leaf[index_].value;
    }

private:
    Node* node_;
    uint16_t index_;
};

} // namespace yudb
