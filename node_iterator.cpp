#include "node_iterator.h"

namespace yudb {

NodeIterator::NodeIterator(NodeFormat* node, uint16_t index) : 
    node_{ node }, index_{ index } {}

NodeIterator::~NodeIterator() = default;

NodeIterator::NodeIterator(const NodeIterator& right) = default;

void NodeIterator::operator=(const NodeIterator& right) {
    assert(node_ == right.node_);
    index_ = right.index_;
}

const NodeIterator& NodeIterator::operator*() const { 
    return *this;
}

NodeIterator& NodeIterator::operator*() { 
    return *this;
}

NodeIterator& NodeIterator::operator--() noexcept {
    --index_;
    return *this;
}

NodeIterator& NodeIterator::operator++() noexcept {
    ++index_;
    return *this;
}

NodeIterator NodeIterator::operator+(const difference_type n) const {
    return NodeIterator{ node_, uint16_t(index_ + n) };
}

NodeIterator NodeIterator::operator-(const difference_type n) const {
    return NodeIterator{ node_, uint16_t(index_ - n) };
}

NodeIterator::difference_type NodeIterator::operator-(const NodeIterator& right) const noexcept {
    return index_ - right.index_;
}

NodeIterator& NodeIterator::operator-=(const difference_type off) noexcept {
    index_ -= off;
    return *this;
}

NodeIterator& NodeIterator::operator+=(const difference_type off) noexcept {
    index_ += off;
    return *this;
}

bool NodeIterator::operator==(const NodeIterator& right) const {
    return node_ == right.node_ && index_ == right.index_;
}

Cell& NodeIterator::key() const {
    if (node_->type == NodeFormat::Type::kBranch) {
        return node_->body.branch[index_].key;
    } else {
        assert(node_->type == NodeFormat::Type::kLeaf);
        return node_->body.leaf[index_].key;
    }
}
Cell& NodeIterator::value() const {
    assert(node_->type == NodeFormat::Type::kLeaf);
    return node_->body.leaf[index_].value;
}

} // namespace yudb