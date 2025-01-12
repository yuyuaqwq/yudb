//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include <yudb/btree_iterator.h>

#include <yudb/btree.h>
#include <yudb/bucket_impl.h>
#include <yudb/tx_impl.h>
#include <yudb/error.h>

namespace yudb {

BTreeIterator::BTreeIterator(BTree* btree) : btree_{ btree } {}

BTreeIterator::BTreeIterator(const BTreeIterator& right) : btree_{right.btree_} {
    operator=(right);
}

void BTreeIterator::operator=(const BTreeIterator& right) {
    btree_ = right.btree_;
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
    if (Empty()) {
        return right.status() == Status::kInvalid;
    }
    else if (right.Empty()) {
        return status() == Status::kInvalid;
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

bool BTreeIterator::is_bucket() const {
    auto [node, slot_id] = GetLeafNode(false);
    return node.IsBucket(slot_id);
}

std::pair<LeafNode&, SlotId> BTreeIterator::GetLeafNode(bool dirty) const {
    if (*this == btree_->end()) {
        throw InvalidArgumentError("invalid iterator.");
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
        Push({ pgid, 0 });
        assert(node.count() > 0);
        BranchNode branch_node{ btree_, node.Release() };
        pgid = branch_node.GetLeftChild(0);
    } while (true);
    Push({ pgid, 0 });
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
        Push({ pgid, node.count() - 1 });
        assert(node.count() > 0);
        BranchNode branch_node{ btree_, node.Release() };
        pgid = branch_node.GetLeftChild(node.count() - 1);
    } while (true);
    Node node{ btree_, pgid, false };
    Push({ pgid, node.count() - 1 });
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
    if (Empty()) {
        pgid = btree_->root_pgid_;
        if (pgid == kPageInvalidId) {
            return false;
        }
    } else {
        auto [parent_pgid, slot_id] = Front();
        assert(cached_node_->page_id() == parent_pgid);
        if (cached_node_->IsLeaf()) {
            return false;
        }
        BranchNode branch_parent{ btree_, cached_node_->Release() };
        pgid = branch_parent.GetLeftChild(slot_id);
    }
    cached_node_.emplace(btree_, pgid, false);

    // 在节点中进行二分查找
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
    Push(std::pair{ pgid, slot_id });
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
    bool copy_needed = false;
    // 自底向上复制页面
    for (ptrdiff_t i = stack_.size() - 1; i >= 0; i--) {
        auto& [pgid, slot_id] = stack_[i];
        Node node{ btree_, pgid, true };
        copy_needed = tx.CopyNeeded(node.last_modified_txid());
        if (!copy_needed) {
            if (node.IsBranch()) {
                BranchNode branch_node{ btree_, node.Release() };
                assert(lower_pgid != kPageInvalidId);
                branch_node.SetLeftChild(slot_id, lower_pgid);
            }
            // 如果不需要复制，则可以提前结束，因为如果是底下的页面不需要复制，那么这条路径的页面一定经过复制了
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