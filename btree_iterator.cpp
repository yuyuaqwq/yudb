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
    auto [node, index] = LeafImmNode();
    return node.leaf_key(index).bucket_flag;
}

void BTreeIterator::set_is_bucket() {
    auto [node, index] = LeafMutNode();
    node.leaf_key(index).bucket_flag = 1;
}

bool BTreeIterator::is_inline_bucket() const {
    auto [node, index] = LeafImmNode();
    return node.leaf_value(index).bucket_flag;
}

void BTreeIterator::set_is_inline_bucket() {
    auto [node, index] = LeafMutNode();
    node.leaf_value(index).bucket_flag = 1;
}


std::pair<ImmNode, uint16_t> BTreeIterator::LeafImmNode() const {
    if (*this == btree_->end()) {
        throw std::runtime_error("invalid iterator.");
    }
    auto& [pgid, index] = Front();
    ImmNode node{ btree_, pgid };
    assert(*this != btree_->end());
    assert(node.IsLeaf());
    return { std::move(node), index };
}

std::pair<MutNode, uint16_t> BTreeIterator::LeafMutNode() const {
    if (*this == btree_->end()) {
        throw std::runtime_error("invalid iterator.");
    }
    auto& [pgid, index] = Front();
    MutNode node{ btree_, pgid };
    assert(*this != btree_->end());
    assert(node.IsLeaf());
    return { std::move(node), index };
}


std::tuple<const uint8_t*, size_t, std::optional<PageReference>>
BTreeIterator::KeyCell() const {
    auto [node, index] = LeafImmNode();
    auto res = node.CellLoad(node.leaf_key(index));
    return res;
}

std::tuple<const uint8_t*, size_t, std::optional<PageReference>>
BTreeIterator::ValueCell() const {
    auto [node, index] = LeafImmNode();
    auto res = node.CellLoad(node.leaf_value(index));
    return res;
}


void BTreeIterator::First(PageId pgid) {
    if (pgid == kPageInvalidId) {
        return;
    }
    status_ = Status::kIter;
    do {
        ImmNode node{ btree_, pgid };
        if (node.IsLeaf()) {
            break;
        }
        stack_.push_back({ pgid, 0 });
        assert(node.element_count() > 0);
        pgid = node.branch_left_child(0);
    } while (true);
    stack_.push_back({ pgid, 0 });
}

void BTreeIterator::Last(PageId pgid) {
    status_ = Status::kIter;
    do {
        ImmNode node{ btree_, pgid };
        if (node.IsLeaf()) {
            break;
        }
        stack_.push_back({ pgid, node.element_count() - 1 });
        assert(node.element_count() > 0);
        pgid = node.branch_left_child(node.element_count() - 1);
    } while (true);
    ImmNode node{ btree_, pgid };
    stack_.push_back({ pgid, node.element_count() - 1 });
}

void BTreeIterator::Next() {
    assert(!Empty());
    do {
        auto& [pgid, index] = Front();
        ImmNode node{ btree_, pgid };

        if (++index < node.element_count()) {
            if (node.IsLeaf()) {
                return;
            }
            First(node.branch_left_child(index));
            return;
        }
        if (node.IsBranch() && index == node.element_count()) {
            First(node.tail_child());
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
        ImmNode node{ btree_, pgid };
        if (index > 0) {
            --index;
            if (node.IsLeaf()) {
                return;
            }
            Last(node.branch_left_child(index));
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
        ImmNode parent{ btree_, parent_pgid };
        if (parent.IsLeaf()) {
            return false;
        }
        pgid = parent.BranchGetLeftChild(pos);
    }
    ImmNode node{ btree_, pgid };

    // 在节点中进行二分查找
    uint16_t index;
    status_ = Status::kNe;
    if (node.element_count() > 0) {
        auto iter = std::lower_bound(node.begin(), node.end(), key, [&](const NodeIterator& iter, std::span<const uint8_t> search_key) -> bool {
            auto [buf, size, ref] = node.CellLoad(iter.key());
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
        if (index == node.element_count() && node.IsLeaf()) {
            status_ = Status::kInvalid;
        }
        if (status_ == Status::kEq && node.IsBranch()) {
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
        MutNode node{ btree_, pgid };
        if (!tx.NeedCopy(node.last_modified_txid())) {
            if (node.IsBranch()) {
                assert(lower_pgid != kPageInvalidId);
                node.BranchSetLeftChild(index, lower_pgid);
            }
            return;
        }

        Node new_node = node.Copy();
        new_node.set_last_modified_txid(tx.txid());
        if (new_node.IsBranch()) {
            assert(lower_pgid != kPageInvalidId);
            new_node.BranchSetLeftChild(index, lower_pgid);
        }
        lower_pgid = new_node.page_id();
        pgid = lower_pgid;
    }
    *btree_->root_pgid_ = lower_pgid;
}


} // namespace yudb