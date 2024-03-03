#include "db/btree_iterator.h"

#include "db/btree.h"
#include "db/bucket_impl.h"
#include "db/tx_impl.h"

namespace yudb {

BTreeIterator::BTreeIterator(BTree* btree) : btree_{ btree } {}
BTreeIterator::BTreeIterator(const BTreeIterator& right) : btree_{right.btree_} {
    operator=(right);
}
void BTreeIterator::operator=(const BTreeIterator& right) {
    assert(btree_ == right.btree_);
    stack_ = right.stack_;
    status_ = right.status_;
    if (right.cached_node_.has_value()) {
        auto node = right.cached_node_->AddReference();
        cached_node_.emplace(btree_, node.Release());
    }
}

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
        } else if (right.Empty()) {
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

std::string_view BTreeIterator::key() const {
    auto span = GetKey();
    return { reinterpret_cast<const char*>(span.data()), span.size() };
}
std::string_view BTreeIterator::value() const {
    auto span = GetValue();
    return { reinterpret_cast<const char*>(span.data()), span.size() };
}


std::pair<LeafNode&, SlotId> BTreeIterator::GetLeafNode(bool dirty) const {
    if (*this == btree_->end()) {
        throw std::runtime_error("invalid iterator.");
    }
    auto& [pgid, slot_id] = Front();
    if (!cached_node_.has_value() || cached_node_->page_id() != pgid) {
        cached_node_.emplace(btree_, pgid, dirty);;
    }
    assert(cached_node_->IsLeaf());
    return { static_cast<LeafNode&>(*cached_node_), slot_id };
}

std::span<const uint8_t> BTreeIterator::GetKey() const {
    auto [node, slot_id] = GetLeafNode(false);
    auto span = node.GetKey(slot_id);
    return span;
}
std::span<const uint8_t> BTreeIterator::GetValue() const {
    auto [node, slot_id] = GetLeafNode(false);
    return node.GetValue(slot_id);
}

void BTreeIterator::First(PageId pgid) {
    if (pgid == kPageInvalidId) {
        return;
    }
    status_ = Status::kIter;
    do {
        Node node{ btree_, pgid, false };
        if (node.IsLeaf()) {
            if (node.count() == 0) {
                return;
            }
            break;
        }
        stack_.push_back({ pgid, 0 });
        assert(node.count() > 0);
        BranchNode branch_node{ btree_, node.Release() };
        pgid = branch_node.GetLeftChild(0);
    } while (true);
    stack_.push_back({ pgid, 0 });
}
void BTreeIterator::Last(PageId pgid) {
    status_ = Status::kIter;
    do {
        Node node{ btree_, pgid, false };
        if (node.IsLeaf()) {
            if (node.count() == 0) {
                return;
            }
            break;
        }
        stack_.push_back({ pgid, node.count() - 1 });
        assert(node.count() > 0);
        BranchNode branch_node{ btree_, node.Release() };
        pgid = branch_node.GetLeftChild(node.count() - 1);
    } while (true);
    Node node{ btree_, pgid, false };
    stack_.push_back({ pgid, node.count() - 1 });
}
void BTreeIterator::Next() {
    assert(!Empty());
    do {
        auto& [pgid, slot_id] = Front();
        Node node{ btree_, pgid, false };

        if (++slot_id < node.count()) {
            if (node.IsLeaf()) {
                return;
            }
            BranchNode branch_node{ btree_, node.Release() };
            First(branch_node.GetLeftChild(slot_id));
            return;
        }
        if (node.IsBranch() && slot_id == node.count()) {
            BranchNode branch_node{ btree_, node.Release() };
            First(branch_node.GetTailChild());
            return;
        }
        Pop();
    } while (!Empty());
}
void BTreeIterator::Prev() {
    if (Empty()) {
        Last(btree_->root_pgid_);
        return;
    }
    do {
        auto& [pgid, slot_id] = Front();
        Node node{ btree_, pgid, false };
        if (slot_id > 0) {
            --slot_id;
            if (node.IsLeaf()) {
                return;
            }
            BranchNode branch_node{ btree_, node.Release() };
            Last(branch_node.GetLeftChild(slot_id));
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
        pgid = btree_->root_pgid_;
        if (pgid == kPageInvalidId) {
            return false;
        }
    }
    else {
        auto [parent_pgid, slot_id] = stack_.front();
        assert(cached_node_->page_id() == parent_pgid);
        if (cached_node_->IsLeaf()) {
            return false;
        }
        BranchNode branch_parent{ btree_, cached_node_->Release() };
        pgid = branch_parent.GetLeftChild(slot_id);
    }
    cached_node_.emplace(btree_, pgid, false);

    // �ڽڵ��н��ж��ֲ���
    SlotId slot_id;
    bool eq;
    status_ = Status::kNe;
    if (cached_node_->count() > 0) {
        auto res = cached_node_->LowerBound(key);
        slot_id = res.first;
        eq = res.second;
        if (eq) {
            status_ = Status::kEq;
        } else if (slot_id == cached_node_->count() && cached_node_->IsLeaf()) {
            status_ = Status::kInvalid;
        }
        if (eq && cached_node_->IsBranch()) {
            ++slot_id;
        }
    } else {
        slot_id = 0;
    }
    stack_.push_back(std::pair{ pgid, slot_id });
    return true;
}

std::pair<PageId, SlotId>& BTreeIterator::Front() {
    return stack_.front();
}
const std::pair<PageId, SlotId>& BTreeIterator::Front() const {
    return stack_.front();
}
void BTreeIterator::Push(const std::pair<PageId, SlotId>& v) {
    stack_.push_back(v);
}
void BTreeIterator::Pop() {
    stack_.pop_back();
}
bool BTreeIterator::Empty() const {
    return stack_.empty();
}

void BTreeIterator::CopyAllPagesByPath() {
    if (Empty()) {
        return;
    }
    auto& tx = btree_->bucket().tx();
    auto lower_pgid = kPageInvalidId;
    for (ptrdiff_t i = stack_.size() - 1; i >= 0; i--) {
        auto& [pgid, slot_id] = stack_[i];
        Node node{ btree_, pgid, true };
        if (!tx.IsLegacyTx(node.last_modified_txid())) {
            if (node.IsBranch()) {
                BranchNode branch_node{ btree_, node.Release() };
                assert(lower_pgid != kPageInvalidId);
                branch_node.SetLeftChild(slot_id, lower_pgid);
            }
            return;
        }

        auto new_node = node.Copy();
        if (new_node.IsBranch()) {
            assert(lower_pgid != kPageInvalidId);
            BranchNode branch_node{ btree_, new_node.Release() };
            branch_node.SetLeftChild(slot_id, lower_pgid);
            lower_pgid = branch_node.page_id();
        } else {
            lower_pgid = new_node.page_id();
        }
        pgid = lower_pgid;
    }
    btree_->root_pgid_ = lower_pgid;
}

} // namespace yudb