//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include <yudb/btree.h>

#include <yudb/bucket_impl.h>
#include <yudb/tx_impl.h>

#include "pager.h"

namespace yudb {

BTree::BTree(BucketImpl* bucket, PageId* root_pgid, Comparator comparator)
    : bucket_(bucket)
    , root_pgid_(*root_pgid)
    , comparator_(comparator) {}

BTree::~BTree() = default;

bool BTree::Empty() const {
    return root_pgid_ == kPageInvalidId;
}

BTree::Iterator BTree::LowerBound(std::span<const uint8_t> key) {
    auto iter = Iterator(this);
    auto continue_ = iter.Top(key);
    while (continue_) {
        continue_ = iter.Down(key);
    }
    return iter;
}

BTree::Iterator BTree::Get(std::span<const uint8_t> key) {
    const auto iter = LowerBound(key);
    if (iter.status() != Iterator::Status::kEq) {
        return Iterator{ this };
    }
    return iter;
}

void BTree::Insert(std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket) {
    auto iter = LowerBound(key);
    iter.CopyAllPagesByPath();
    Put(&iter, key, value, true, is_bucket);
}

void BTree::Put(std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket) {
    auto iter = LowerBound(key);
    iter.CopyAllPagesByPath();
    Put(&iter, key, value, false, is_bucket);
}

void BTree::Update(Iterator* iter, std::span<const uint8_t> value) {
    auto [pgid, slot_id] = iter->Front();
    auto node = LeafNode(this, pgid, true);
    assert(node.IsLeaf());
    node.Update(slot_id, node.GetKey(slot_id), value);
}

bool BTree::Delete(std::span<const uint8_t> key) {
    auto iter = LowerBound(key);
    if (iter.status() != Iterator::Status::kEq) {
        return false;
    }
    iter.CopyAllPagesByPath();
    Delete(&iter);
    return true;
}

BTree::Iterator BTree::begin() noexcept {
    auto iter = Iterator(this);
    iter.First(root_pgid_);
    return iter;
}

BTree::Iterator BTree::end() noexcept {
    return Iterator(this);
}

//void BTree::Print(bool str, PageId pgid, int level) {
//    const std::string indent(level * 4, ' ');
//    Node node{ this, pgid, false };
//    if (node.IsBranch()) {
//        //node.BranchCheck();
//        BranchNode node{ this, pgid, false };
//        Print(str, node.GetTailChild(), level + 1);
//        for (int i = node.count() - 1; i >= 0; i--) {
//            std::string key_str;
//            auto key = node.GetKey(i);
//            if (str) {
//                std::cout << std::format("{}branch[{}]::key::{}::level::{}\n", indent, pgid, std::string_view{ reinterpret_cast<const char*>(key.data()), key.size()}, level);
//            }
//            else {
//                for (uint32_t j = 0; j < key.size(); j++) {
//                    key_str += std::format("{:02x}", key.data()[j]) + " ";
//                }
//                std::cout << std::format("{}branch[{}]::key::{}::level::{}\n", indent, pgid, key_str, level);
//            }
//            Print(str, node.GetLeftChild(i), level + 1);
//        }
//    } else {
//        assert(node.IsLeaf());
//        //node.LeafCheck();
//        LeafNode node{ this, pgid, false };
//        for (int i = node.count() - 1; i >= 0; i--) {
//            std::string key_str, value_str;
//            auto key = node.GetKey(i);
//            auto value = node.GetValue(i);
//            if (str) {
//                std::cout << std::format("{}leaf[{}]::key::{}::value::{}::level::{}\n", indent, pgid, std::string_view{ reinterpret_cast<const char*>(key.data()), key.size()}, std::string_view{reinterpret_cast<const char*>(value.data()), value.size()}, level);
//            }
//            else {
//                for (uint32_t j = 0; j < key.size(); j++) {
//                    key_str += std::format("{:02x}", key.data()[j]) + " ";
//                }
//                for (uint32_t j = 0; j < value.size(); j++) {
//                    value_str += std::format("{:02x}", value.data()[j]) + " ";
//                }
//                std::cout << std::format("{}leaf[{}]::key::{}::value::{}::level::{}\n", indent, pgid, key_str, value_str, level);
//            }
//        }
//    }
//}
//
//void BTree::Print(bool str) {
//    if (root_pgid_ == kPageInvalidId) {
//        return;
//    }
//    Print(str, root_pgid_, 0);
//}

std::tuple<BranchNode, SlotId, PageId, bool> BTree::GetSibling(Iterator* iter) {
    auto [parent_pgid, parent_slot_id] = iter->Front();
    auto parent = BranchNode(this, parent_pgid, true);

    bool left_sibling = false;
    PageId sibling_pgid;
    if (parent_slot_id == parent.count()) {
        // If it is the largest element in the parent node, 
        // only the left sibling node can be chosen.
        left_sibling = true;
        sibling_pgid = parent.GetLeftChild(parent_slot_id - 1);
    } else {
        sibling_pgid = parent.GetRightChild(parent_slot_id);
    }
    iter->Pop();
    return { std::move(parent), parent_slot_id, sibling_pgid, left_sibling };
}

void BTree::Merge(BranchNode&& left, BranchNode&& right, std::span<const uint8_t> down_key) {
    bool success = left.Append(down_key, kPageInvalidId, true);
    assert(success);
    for (SlotId i = 0; i < right.count(); i++) {
        bool success = left.Append(right.GetKey(i), right.GetLeftChild(i), false);
        assert(success);
    }
    left.SetTailChild(right.GetTailChild());
    right.Destroy();
    bucket_->pager().Free(right.page_id(), 1);
}

void BTree::Delete(Iterator* iter, BranchNode&& node, SlotId left_del_slot_id) {
    node.Delete(left_del_slot_id, true);

    if (iter->Empty()) {
        // If there is no parent node
        // Check if there are no child nodes; if true, change the last remaining child node to the root node
        if (node.count() == 0) {
            assert(node.page_id() == root_pgid_);
            auto old_root = root_pgid_;
            root_pgid_ = node.GetTailChild();
            node.Destroy();
            bucket_->pager().Free(old_root, 1);
        }
        return;
    }

    if (node.GetFillRate() >= 0.4) {
        return;
    }

    auto [parent, parent_slot_id, sibling_id, left_sibling] = GetSibling(iter);
    if (left_sibling) --parent_slot_id;

    auto sibling = BranchNode(this, sibling_id, true);
    if (bucket_->tx().CopyNeeded(sibling.last_modified_txid())) {
        sibling = BranchNode{ this, sibling.Copy().Release() };
        if (left_sibling) {
            parent.SetLeftChild(parent_slot_id, sibling.page_id());
        } else {
            parent.SetLeftChild(parent_slot_id + 1, sibling.page_id());
        }
    }
    if (sibling.GetFillRate() > 0.5 && sibling.count() >= 2) {
        // If the sibling node's fill rate is sufficient
        std::span<const uint8_t> new_key;
        if (left_sibling) {
            // The last element of the left sibling node rises to the specified position in the parent node
            // The corresponding element in the parent node descends to the head of the current node
            // The right child of the rising element is attached to the left side of the descending parent node element

            //                                27
            //      12          17                               37
            // 1 5      10 15       20 25                30 35         40 45

            //                       17
            //      12                                    27            37
            // 1 5      10 15                    20 25         30 35         40 45
            bool success = node.Insert(0, parent.GetKey(parent_slot_id), sibling.GetTailChild(), false);
            assert(success);
            new_key = sibling.GetKey(sibling.count() - 1);
            // Deletion does not trigger Compactify, so it can be deleted first
            sibling.Pop(true);
        } else {
            parent.SetLeftChild(parent_slot_id + 1, sibling.page_id());
            // The first element of the right sibling node rises to the specified position in the parent node
            // The corresponding element in the parent node descends to the tail of the current node
            // The left child of the rising element is attached to the right side of the descending parent node element

            //                       17
            //      12                                    27            37
            // 1 5      10 15                    20 25         30 35         40 45

            //                                27
            //      12          17                               37
            // 1 5      10 15       20 25                30 35         40 45
            bool success = node.Append(parent.GetKey(parent_slot_id), sibling.GetLeftChild(0), true);
            assert(success);
            new_key = sibling.GetKey(0);
            // Deletion does not trigger Compactify, so it can be deleted first
            sibling.Delete(0, false);
        }
        bool success = parent.Update(parent_slot_id, new_key);
        if (success == false) {
            // If there is not enough space in the parent node to update the key, delete the corresponding element in the parent node and convert it to an upward insertion
            parent.Delete(parent_slot_id, true);
            iter->Push({ parent.page_id(), parent_slot_id });
            if (left_sibling) {
                Put(iter, std::move(sibling), std::move(node), new_key);
            } else {
                Put(iter, std::move(node), std::move(sibling), new_key);
            }
        }
        return;
    }

    // Merge
    if (left_sibling) {
        Merge(std::move(sibling), std::move(node), parent.GetKey(parent_slot_id));
    } else {
        Merge(std::move(node), std::move(sibling), parent.GetKey(parent_slot_id));
    }

    // Delete the parent element upwards
    Delete(iter, std::move(parent), parent_slot_id);
}

void BTree::Merge(LeafNode&& left, LeafNode&& right) {
    for (size_t i = 0; i < right.count(); i++) {
        bool success = left.Append(right.GetKey(i), right.GetValue(i));
        assert(success);
        left.SetIsBucket(left.count() - 1, right.IsBucket(i));
    }
    right.Destroy();
    bucket_->pager().Free(right.page_id(), 1);
}

void BTree::Delete(Iterator* iter) {
    assert(!iter->is_bucket() || iter->is_bucket() && iter->value<PageId>() == kPageInvalidId);

    auto [pgid, pos] = iter->Front();
    auto node = LeafNode(this, pgid, true);
    node.Delete(pos);
    if (node.GetFillRate() >= 0.4) {
        return;
    }

    iter->Pop();
    if (iter->Empty()) {
        // No parent node, skip if it's a leaf node
        return;
    }

    auto [parent, parent_slot_id, sibling_id, left_sibling] = GetSibling(iter);
    if (left_sibling) --parent_slot_id;

    auto sibling = LeafNode(this, sibling_id, true);
    if (bucket_->tx().CopyNeeded(sibling.last_modified_txid())) {
        sibling = LeafNode(this, sibling.Copy().Release());
        if (left_sibling) {
            parent.SetLeftChild(parent_slot_id, sibling.page_id());
        } else {
            parent.SetLeftChild(parent_slot_id + 1, sibling.page_id());
        }
    }

    // Sibling node has sufficient fill rate
    if (sibling.GetFillRate() > 0.5 && sibling.count() >= 2) {
        std::span<const uint8_t> new_key;
        if (left_sibling) {
            // The last element of the left sibling is inserted at the head of the current node
            // Update the parent's key to the new first key of the current node
            SlotId tail_slot_id = sibling.count() - 1;
            auto success = node.Insert(0, sibling.GetKey(tail_slot_id), sibling.GetValue(tail_slot_id));
            assert(success);
            node.SetIsBucket(0, sibling.IsBucket(tail_slot_id));
            sibling.Pop();
            new_key = node.GetKey(0);
        } else {
            // The first element of the right sibling is inserted at the tail of the current node
            // Update the parent's key to the new first key of the right sibling
            auto success = node.Append(sibling.GetKey(0), sibling.GetValue(0));
            assert(success);
            node.SetIsBucket(node.count() - 1, sibling.IsBucket(0));
            sibling.Delete(0);
            new_key = sibling.GetKey(0);
        }
        bool success = parent.Update(parent_slot_id, new_key, parent.GetLeftChild(parent_slot_id), false);
        if (success == false) {
            // The parent node lacks space to update the key, delete the corresponding element in the parent node,
            // and convert it into an upward insertion
            parent.Delete(parent_slot_id, true);
            iter->Push({ parent.page_id(), parent_slot_id });
            if (left_sibling) {
                Put(iter, std::move(sibling), std::move(node), new_key);
            } else {
                Put(iter, std::move(node), std::move(sibling), new_key);
            }
        }
        return;
    }

    // Merge
    if (left_sibling) {
        Merge(std::move(sibling), std::move(node));
    } else {
        Merge(std::move(node), std::move(sibling));
    }

    // Delete the parent element upwards
    Delete(iter, std::move(parent), parent_slot_id);
}

std::tuple<std::span<const uint8_t>, BranchNode> BTree::Split(BranchNode* left, SlotId insert_slot_id, std::span<const uint8_t> insert_key, PageId insert_right_child) {
    assert(insert_slot_id <= left->count());
    
    auto right = BranchNode(this, bucket_->pager().Alloc(1), true);
    right.Build(kPageInvalidId);

    auto saved_left_count = left->count();
    assert(saved_left_count >= 2);
    assert(left->GetFillRate() > 0.5);
    
    PageId insert_left_child = left->GetLeftChild(insert_slot_id);
    left->SetLeftChild(insert_slot_id, insert_right_child);

    for (int16_t i = saved_left_count - 1; i >= 0; --i) {
        auto success = right.Append(left->GetKey(i), left->GetLeftChild(i), false);
        assert(success);
        left->Pop(false);
        if (left->GetFillRate() <= 0.5) {
            break;
        }
    }
    std::reverse(right.slots(), right.slots() + right.count());
    assert(left->GetFillRate() <= 0.5);

    right.SetTailChild(left->GetTailChild());

    if (insert_slot_id > left->count()) {
        auto success = right.Insert(insert_slot_id - left->count(), insert_key, insert_left_child, false);
        if (!success) {
            success = left->Append(right.GetKey(0), right.GetLeftChild(0), true);
            assert(success);
            right.Delete(0, false);
            success = right.Insert(insert_slot_id - left->count(), insert_key, insert_left_child, false);
            assert(success);
        }
    } else {
        auto success = left->Insert(insert_slot_id, insert_key, insert_left_child, false);
        assert(success);
    }
    
    std::span<const uint8_t> up_key;
    if (left->count() >= 2) {
        assert(right.count() >= 1);
        // The last element of the left side rises
        up_key = left->GetKey(left->count() - 1);
        left->Pop(true);
        
    } else {
        assert(right.count() >= 2);
        // The first element of the right side rises
        up_key = right.GetKey(0);
        auto left_child = right.GetLeftChild(0);
        right.Delete(0, false);
        left->SetTailChild(left_child);
    }
    return { up_key, std::move(right) };
}

void BTree::Put(Iterator* iter, Node&& left, Node&& right, std::span<const uint8_t> key) {
    if (iter->Empty()) {
        root_pgid_ = bucket_->pager().Alloc(1);
        auto node = BranchNode(this, root_pgid_, true);
        node.Build(left.page_id());
        node.Append(key, right.page_id(), true);
        return;
    }

    auto [pgid, slot_id] = iter->Front();
    auto node = BranchNode(this, pgid, true);

    //      3       7
    // 1 2     3 4     7 8

    //      3       5    7 
    // 1 2     3 4     5    7 8
    if (node.Insert(slot_id, key, right.page_id(), true)) {
        return;
    }

    auto [branch_key, branch_right] = Split(&node, slot_id, key, right.page_id());
    iter->Pop();
    Put(iter, std::move(node), std::move(branch_right), branch_key);
}

LeafNode BTree::Split(LeafNode* left, SlotId insert_slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket) {
    assert(insert_slot_id <= left->count());

    auto right = LeafNode(this, bucket_->pager().Alloc(1), true);
    right.Build();

    auto saved_left_count = left->count();
    assert(saved_left_count >= 2);
    assert(left->GetFillRate() > 0.5);

    for (SlotId i = saved_left_count - 1; i >= 0; --i) {
        auto success = right.Append(left->GetKey(i), left->GetValue(i));
        assert(success);
        right.SetIsBucket(right.count() - 1, left->IsBucket(i));
        left->Pop();
        if (left->GetFillRate() <= 0.5) {
            break;
        }
    }
    std::reverse(right.slots(), right.slots() + right.count());
    assert(left->GetFillRate() <= 0.5);

    // If the fill rate of the node is >50%, the insertion may fail
    if (insert_slot_id > left->count()) {
        // If it fails, move the first element to the left side and retry
        assert(insert_slot_id != left->count());
        auto success = right.Insert(insert_slot_id - left->count(), key, value);
        if (!success) {
            success = left->Append(right.GetKey(0), right.GetValue(0));
            assert(success);
            left->SetIsBucket(left->count() - 1, right.IsBucket(0));
            right.Delete(0);
            success = right.Insert(insert_slot_id - left->count(), key, value);
            assert(success);
        } 
        right.SetIsBucket(insert_slot_id - left->count(), is_bucket);
    } else {
        auto success = left->Insert(insert_slot_id, key, value);
        assert(success);
        left->SetIsBucket(insert_slot_id, is_bucket);
    }
    
    assert(left->count() >= 1 && right.count() >= 2 || left->count() >= 2 && right.count() >= 1);
    return right;
}

void BTree::Put(Iterator* iter, std::span<const uint8_t> key, std::span<const uint8_t> value, bool insert_only, bool is_bucket) {
    if (iter->Empty()) {
        root_pgid_ = bucket_->pager().Alloc(1);
        auto node = LeafNode(this, root_pgid_, true);
        node.Build();
        auto success = node.Insert(0, key, value); assert(success == true);
        node.SetIsBucket(0, is_bucket);
        return;
    }

    auto [pgid, slot_id] = iter->Front();
    auto node = LeafNode(this, pgid, true);
    if (!insert_only && iter->status() == Iterator::Status::kEq) {
        assert(iter->is_bucket() == is_bucket);
        node.Update(slot_id, key, value);
        node.SetIsBucket(slot_id, is_bucket);
        return;
    }

    if (node.Insert(slot_id, key, value)) {
        node.SetIsBucket(slot_id, is_bucket);
        return;
    }

    // Needs to split and then insert upward
    LeafNode right = Split(&node, slot_id, key, value, is_bucket);
    
    iter->Pop();
    // Promote the first key of the right node
    Put(iter, std::move(node), std::move(right), right.GetKey(0));
}

} // namespace yudb