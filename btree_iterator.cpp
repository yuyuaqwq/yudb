#include "btree_iterator.h"

#include "btree.h"
#include "bucket_impl.h"
#include "tx_impl.h"

namespace yudb {

BTreeIterator::BTreeIterator(BTree* btree) : btree_{ btree } {}
BTreeIterator::BTreeIterator(const BTreeIterator& right) = default;
void BTreeIterator::operator=(const BTreeIterator& right) {
    assert(btree_ == right.btree_);
    stack_ = right.stack_;
    status_ = right.status_;
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

std::string BTreeIterator::key() const {
    auto [span, ref] = KeySlot();
    std::string key;
    if (ref.has_value()) {
        auto ref_str = std::get_if<std::string>(&*ref);
        if (ref_str) {
            key = std::move(*ref_str);
        }
    }
    if (key.empty()) {
        key = { reinterpret_cast<const char*>(span.data()), span.size() };
    }
    return key;
}
std::string BTreeIterator::value() const {
    auto [span, ref] = ValueSlot();
    std::string val;
    if (ref.has_value()) {
        auto ref_str = std::get_if<std::string>(&*ref);
        if (ref_str) {
            val = std::move(*ref_str);
        }
    }
    if (val.empty()) {
        val = { reinterpret_cast<const char*>(span.data()), span.size() };
    }
    return val;
}

bool BTreeIterator::is_bucket() const {
    auto [node, slot_id] = GetLeafNode(false);
    return node.GetSlot(slot_id).bucket;
}
void BTreeIterator::set_is_bucket() {
    auto [node, slot_id] = GetLeafNode(true);
    node.GetSlot(slot_id).bucket = 1;
}
//bool BTreeIterator::is_inline_bucket() const {
//    auto [node, slot_id] = GetLeafNode();
//    return node.GetSlot(slot_id).bucket_flag;
//}
//void BTreeIterator::set_is_inline_bucket() {
//    auto [node, slot_id] = GetLeafNode();
//    node.leaf_value(slot_id).bucket_flag = 1;
//}

std::pair<LeafNode, SlotId> BTreeIterator::GetLeafNode(bool dirty) const {
    if (*this == btree_->end()) {
        throw std::runtime_error("invalid iterator.");
    }
    auto& [pgid, slot_id] = Front();
    LeafNode node{ btree_, pgid, dirty };
    assert(*this != btree_->end());
    assert(node.IsLeaf());
    return { std::move(node), slot_id };
}

std::tuple<std::span<const uint8_t>, std::optional<std::variant<ConstPage, std::string>>>
BTreeIterator::KeySlot() const {
    auto [node, slot_id] = GetLeafNode(false);
    auto res = node.GetKey(slot_id);
    return { res, node.Release() };
}
std::tuple<std::span<const uint8_t>, std::optional<std::variant<ConstPage, std::string>>>
BTreeIterator::ValueSlot() const {
    auto [node, slot_id] = GetLeafNode(false);
    auto res = node.GetValue(slot_id);
    return { res, node.Release() };
}

void BTreeIterator::First(PageId pgid) {
    if (pgid == kPageInvalidId) {
        return;
    }
    status_ = Status::kIter;
    do {
        Node node{ btree_, pgid, false };
        if (node.IsLeaf()) {
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
        Node parent{ btree_, parent_pgid, false };
        if (parent.IsLeaf()) {
            return false;
        }
        BranchNode branch_parent{ btree_, parent.Release() };
        pgid = branch_parent.GetLeftChild(slot_id);
    }
    Node node{ btree_, pgid, false };

    // 在节点中进行二分查找
    SlotId slot_id;
    bool eq;
    status_ = Status::kNe;
    if (node.count() > 0) {
        if (node.IsBranch()) {
            BranchNode branch_node{ btree_, node.Release() };
            auto res = branch_node.LowerBound(key);
            slot_id = res.first;
            eq = res.second;
        } else {
            assert(node.IsLeaf());
            LeafNode leaf_node{ btree_, node.Release() };
            auto res = leaf_node.LowerBound(key);
            slot_id = res.first;
            eq = res.second;
        }
        if (eq) {
            status_ = Status::kEq;
        }
        else if (slot_id == node.count() && node.IsLeaf()) {
            status_ = Status::kInvalid;
        }
        if (eq && node.IsBranch()) {
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

void BTreeIterator::PathCopy() {
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