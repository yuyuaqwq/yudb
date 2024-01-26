#include "btree_iterator.h"

#include "btree.h"
#include "bucket.h"
#include "tx.h"

namespace yudb {

BTreeIterator::reference BTreeIterator::operator*() const noexcept {
    return *this;
}

const BTreeIterator* BTreeIterator::operator->() const noexcept {
    return this;
}

BTreeIterator& BTreeIterator::operator++() noexcept {
    Next();
    return *this;
}

BTreeIterator BTreeIterator::operator++(int) noexcept {
    BTreeIterator tmp = *this;
    tmp.Next();
    return tmp;
}

BTreeIterator& BTreeIterator::operator--() noexcept {
    Prev();
    return *this;
}

BTreeIterator BTreeIterator::operator--(int) noexcept {
    BTreeIterator tmp = *this;
    tmp.Prev();
    return tmp;
}

bool BTreeIterator::operator==(const BTreeIterator& right) const noexcept {
    if (btree_ != right.btree_) {
        return false;
    }
    if (Empty() || right.Empty()) {
        if (Empty()) {
            if (right.Empty()) {
                return true;
            }
            if (right.status() == Status::kInvalid) {
                return true;
            }
        }
        else if (right.Empty()) {
            if (status() == Status::kInvalid) {
                return true;
            }
        }
        return false;
    }
    auto& [pgid, index] = Front();
    auto& [pgid2, index2] = right.Front();
    return pgid == pgid2 && index == index2;
}



std::string BTreeIterator::key() const {
    auto [buf, size, ref] = KeyCell();
    std::string val{ reinterpret_cast<const char*>(buf), size };
    return val;
}

std::string BTreeIterator::value() const {
    auto [buf, size, ref] = ValueCell();
    std::string val{ reinterpret_cast<const char*>(buf), size };
    return val;
}

bool BTreeIterator::is_bucket() const {
    auto [node_operator, index] = LeafImmNodeOperator();
    return node_operator.node().body.leaf[index].key.bucket_flag;
}

void BTreeIterator::set_is_bucket() {
    auto [node_operator, index] = LeafMutNodeOperator();
    node_operator.node().body.leaf[index].key.bucket_flag = 1;
}

bool BTreeIterator::is_inline_bucket() const {
    auto [node_operator, index] = LeafImmNodeOperator();
    return node_operator.node().body.leaf[index].value.bucket_flag;
}

void BTreeIterator::set_is_inline_bucket() {
    auto [node_operator, index] = LeafMutNodeOperator();
    node_operator.node().body.leaf[index].value.bucket_flag = 1;
}


std::pair<ImmNodeOperator, uint16_t> BTreeIterator::LeafImmNodeOperator() const {
    if (*this == btree_->end()) {
        throw std::runtime_error("invalid iterator.");
    }
    auto& [pgid, index] = Front();
    ImmNodeOperator node_operator{ btree_, pgid };
    assert(*this != btree_->end());
    assert(node_operator.IsLeaf());
    return { std::move(node_operator), index };
}

std::pair<MutNodeOperator, uint16_t> BTreeIterator::LeafMutNodeOperator() const {
    if (*this == btree_->end()) {
        throw std::runtime_error("invalid iterator.");
    }
    auto& [pgid, index] = Front();
    MutNodeOperator node_operator{ btree_, pgid };
    assert(*this != btree_->end());
    assert(node_operator.IsLeaf());
    return { std::move(node_operator), index };
}


std::tuple<const uint8_t*, size_t, std::optional<PageReference>>
BTreeIterator::KeyCell() const {
    auto [node_operator, index] = LeafImmNodeOperator();
    auto res = node_operator.CellLoad(node_operator.node().body.leaf[index].key);
    return res;
}

std::tuple<const uint8_t*, size_t, std::optional<PageReference>>
BTreeIterator::ValueCell() const {
    auto [node_operator, index] = LeafImmNodeOperator();
    auto res = node_operator.CellLoad(node_operator.node().body.leaf[index].value);
    return res;
}


void BTreeIterator::First(PageId pgid) {
    if (pgid == kPageInvalidId) {
        return;
    }
    status_ = Status::kIter;
    do {
        ImmNodeOperator node_operator{ btree_, pgid };
        auto& node = node_operator.node();
        if (node_operator.IsLeaf()) {
            break;
        }
        stack_.push_back({ pgid, 0 });
        assert(node.element_count > 0);
        pgid = node.body.branch[0].left_child;
    } while (true);
    ImmNodeOperator node_operator{ btree_, pgid };
    auto& node = node_operator.node();
    stack_.push_back({ pgid, 0 });
}

void BTreeIterator::Last(PageId pgid) {
    status_ = Status::kIter;
    do {
        ImmNodeOperator node_operator{ btree_, pgid };
        auto& node = node_operator.node();
        if (node_operator.IsLeaf()) {
            break;
        }
        stack_.push_back({ pgid, node.element_count - 1 });
        assert(node.element_count > 0);
        pgid = node.body.branch[node.element_count - 1].left_child;
    } while (true);
    ImmNodeOperator node_operator{ btree_, pgid };
    auto& node = node_operator.node();
    stack_.push_back({ pgid, node.element_count - 1 });
}

void BTreeIterator::Next() {
    assert(!Empty());
    do {
        auto& [pgid, index] = Front();
        ImmNodeOperator node_operator{ btree_, pgid };
        auto& node = node_operator.node();

        if (++index < node.element_count) {
            if (node_operator.IsLeaf()) {
                return;
            }
            First(node.body.branch[index].left_child);
            return;
        }
        if (node_operator.IsBranch() && index == node.element_count) {
            First(node.body.tail_child);
            return;
        }
        Pop();
    } while (!Empty());
}

void BTreeIterator::Prev() {
    if (Empty()) {
        Last(*btree_->root_pgid_);
        return;
    }
    do {
        auto& [pgid, index] = Front();
        ImmNodeOperator node_operator{ btree_, pgid };
        auto& node = node_operator.node();
        if (index > 0) {
            --index;
            if (node_operator.IsLeaf()) {
                return;
            }
            Last(node.body.branch[index].left_child);
            return;
        }
        Pop();
    } while (!Empty());
}


bool BTreeIterator::Top(std::span<const uint8_t> key) {
    return Down(key);
}

bool BTreeIterator::Down(std::span<const uint8_t> key) {
    PageId pgid;
    if (stack_.empty()) {
        pgid = *btree_->root_pgid_;
        if (pgid == kPageInvalidId) {
            return false;
        }
    }
    else {
        auto [parent_pgid, pos] = stack_.front();
        ImmNodeOperator parent{ btree_, parent_pgid };
        auto& parent_node = parent.node();
        if (parent.IsLeaf()) {
            return false;
        }
        pgid = parent.BranchGetLeftChild(pos);
    }
    ImmNodeOperator node_operator{ btree_, pgid };
    auto& node = node_operator.node();

    // 在节点中进行二分查找
    uint16_t index;
    status_ = Status::kNe;
    if (node.element_count > 0) {
        auto iter = std::lower_bound(node_operator.begin(), node_operator.end(), key, [&](const NodeIterator& iter, std::span<const uint8_t> search_key) -> bool {
            auto [buf, size, ref] = node_operator.CellLoad(iter.key());
            auto res = btree_->comparator_({ buf, size }, search_key);
            if (res == 0 && size != search_key.size()) {
                return size < search_key.size();
            }
            if (res != 0) {
                return res < 0;
            }
            else {
                status_ = Status::kEq;
                return false;
            }
        });
        index = iter.index();
        if (index == node.element_count && node_operator.IsLeaf()) {
            status_ = Status::kInvalid;
        }
        if (status_ == Status::kEq && node_operator.IsBranch()) {
            ++index;
        }
    }
    else {
        index = 0;
    }
    stack_.push_back(std::pair{ pgid, index });
    return true;
}


std::pair<PageId, uint16_t>& BTreeIterator::Front() {
    return stack_.front();
}

const std::pair<PageId, uint16_t>& BTreeIterator::Front() const {
    return stack_.front();
}


void BTreeIterator::Pop() {
    stack_.pop_back();
}

bool BTreeIterator::Empty() const {
    return stack_.empty();
}



void BTreeIterator::PathCopy() {
    if (Empty()) {
        return;
    }
    auto& tx = btree_->bucket().tx();
    auto lower_pgid = kPageInvalidId;
    for (ptrdiff_t i = stack_.size() - 1; i >= 0; i--) {
        auto& [pgid, index] = stack_[i];
        MutNodeOperator node_operator{ btree_, pgid };
        if (!tx.NeedCopy(node_operator.node().last_modified_txid)) {
            if (node_operator.IsBranch()) {
                assert(lower_pgid != kPageInvalidId);
                node_operator.BranchSetLeftChild(index, lower_pgid);
            }
            return;
        }

        NodeOperator new_node_operator = node_operator.Copy();
        new_node_operator.node().last_modified_txid = tx.txid();
        if (new_node_operator.IsBranch()) {
            assert(lower_pgid != kPageInvalidId);
            new_node_operator.BranchSetLeftChild(index, lower_pgid);
        }
        lower_pgid = new_node_operator.page_id();
        pgid = lower_pgid;
    }
    *btree_->root_pgid_ = lower_pgid;
}


} // namespace yudb