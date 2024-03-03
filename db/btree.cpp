#include "yudb/btree.h"

#include "yudb/tx_impl.h"
#include "yudb/bucket_impl.h"
#include "yudb/pager.h"

namespace yudb {

BTree::BTree(BucketImpl* bucket, PageId* root_pgid, Comparator comparator) :
    bucket_{ bucket },
    root_pgid_ { *root_pgid },
    comparator_{ comparator } {}


BTree::Iterator BTree::LowerBound(std::span<const uint8_t> key) {
    Iterator iter{ this };
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
void BTree::Insert(std::span<const uint8_t> key, std::span<const uint8_t> value) {
    auto iter = LowerBound(key);
    iter.CopyAllPagesByPath();
    Put(&iter, key, value, true);
}
void BTree::Put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
    auto iter = LowerBound(key);
    iter.CopyAllPagesByPath();
    Put(&iter, key, value, false);
}
void BTree::Update(Iterator* iter, std::span<const uint8_t> value) {
    auto [pgid, slot_id] = iter->Front();
    LeafNode node{ this, pgid, true };
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
    Iterator iter{ this };
    iter.First(root_pgid_);
    return iter;
}
BTree::Iterator BTree::end() noexcept {
    return Iterator{ this };
}

void BTree::Print(bool str, PageId pgid, int level) {
    const std::string indent(level * 4, ' ');
    Node node{ this, pgid, false };
    if (node.IsBranch()) {
        //node.BranchCheck();
        BranchNode node{ this, pgid, false };
        Print(str, node.GetTailChild(), level + 1);
        for (int i = node.count() - 1; i >= 0; i--) {
            std::string key_str;
            auto key = node.GetKey(i);
            if (str) {
                std::cout << std::format("{}branch[{}]::key::{}::level::{}\n", indent, pgid, std::string_view{ reinterpret_cast<const char*>(key.data()), key.size()}, level);
            }
            else {
                for (uint32_t j = 0; j < key.size(); j++) {
                    key_str += std::format("{:02x}", key.data()[j]) + " ";
                }
                std::cout << std::format("{}branch[{}]::key::{}::level::{}\n", indent, pgid, key_str, level);
            }
            Print(str, node.GetLeftChild(i), level + 1);
        }
    } else {
        assert(node.IsLeaf());
        //node.LeafCheck();
        LeafNode node{ this, pgid, false };
        for (int i = node.count() - 1; i >= 0; i--) {
            std::string key_str, value_str;
            auto key = node.GetKey(i);
            auto value = node.GetValue(i);
            if (str) {
                std::cout << std::format("{}leaf[{}]::key::{}::value::{}::level::{}\n", indent, pgid, std::string_view{ reinterpret_cast<const char*>(key.data()), key.size()}, std::string_view{reinterpret_cast<const char*>(value.data()), value.size()}, level);
            }
            else {
                for (uint32_t j = 0; j < key.size(); j++) {
                    key_str += std::format("{:02x}", key.data()[j]) + " ";
                }
                for (uint32_t j = 0; j < value.size(); j++) {
                    value_str += std::format("{:02x}", value.data()[j]) + " ";
                }
                std::cout << std::format("{}leaf[{}]::key::{}::value::{}::level::{}\n", indent, pgid, key_str, value_str, level);
            }
        }
    }
}

void BTree::Print(bool str) {
    if (root_pgid_ == kPageInvalidId) {
        return;
    }
    Print(str, root_pgid_, 0);
}



std::tuple<BranchNode, SlotId, PageId, bool> BTree::GetSibling(Iterator* iter) {
    auto [parent_pgid, parent_slot_id] = iter->Front();
    BranchNode parent{ this, parent_pgid, true };

    bool left_sibling = false;
    PageId sibling_pgid;
    if (parent_slot_id == parent.count()) {
        // 是父节点中最大的元素，只能选择左兄弟节点
        left_sibling = true;
        sibling_pgid = parent.GetLeftChild(parent_slot_id - 1);
    }
    else {
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
        // 如果没有父节点
        // 判断是否没有任何子节点了，是则变更余下最后一个子节点为根节点
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

    BranchNode sibling{ this, sibling_id, true };

    if (bucket_->tx().IsLegacyTx(sibling.last_modified_txid())) {
        sibling = BranchNode{ this, sibling.Copy().Release() };
        if (left_sibling) {
            parent.SetLeftChild(parent_slot_id, sibling.page_id());
        } else {
            parent.SetLeftChild(parent_slot_id + 1, sibling.page_id());
        }
    }
    if (sibling.GetFillRate() > 0.5 && sibling.count() >= 2) {
        // 若兄弟节点内填充率充足
        std::span<const uint8_t> new_key;
        if (left_sibling) {
            // 左兄弟节点的末尾元素上升到父节点指定位置
            // 父节点的对应元素下降到当前节点的头部
            // 上升元素其右子节点挂在下降的父节点元素的左侧

            //                                27
            //      12          17                               37
            // 1 5      10 15       20 25                30 35         40 45

            //                       17
            //      12                                    27            37
            // 1 5      10 15                    20 25         30 35         40 45
            bool success = node.Insert(0, parent.GetKey(parent_slot_id), sibling.GetTailChild(), false);
            assert(success);
            new_key = sibling.GetKey(sibling.count() - 1);
            // after deletion, it will not affect the use of span, so we can do this
            sibling.Pop(true);
        } else {
            parent.SetLeftChild(parent_slot_id + 1, sibling.page_id());
            // 右兄弟节点的头元素上升到父节点的指定位置
            // 父节点的对应元素下降到当前节点的尾部
            // 上升元素其左子节点挂在下降的父节点元素的右侧

            //                       17
            //      12                                    27            37
            // 1 5      10 15                    20 25         30 35         40 45

            //                                27
            //      12          17                               37
            // 1 5      10 15       20 25                30 35         40 45
            bool success = node.Append(parent.GetKey(parent_slot_id), sibling.GetLeftChild(0), true);
            assert(success);
            new_key = sibling.GetKey(0);
            // after deletion, it will not affect the use of span, so we can do this
            sibling.Delete(0, false);
        }
        bool success = parent.Update(parent_slot_id, new_key);
        //bool success = false;
        if (success == false) {
            // 父节点内的空间不足以更新key，删除父节点对应的元素，将其转换为向上插入
            parent.Delete(parent_slot_id, true);
            iter->Push({ parent.page_id(), parent_slot_id });
            if (left_sibling) {
                Put(iter, std::move(sibling), std::move(node), new_key, false);
            } else {
                Put(iter, std::move(node), std::move(sibling), new_key, false);
            }
        }
        return;
    }

    // 合并
    if (left_sibling) {
        Merge(std::move(sibling), std::move(node), parent.GetKey(parent_slot_id));
    } else {
        Merge(std::move(node), std::move(sibling), parent.GetKey(parent_slot_id));
    }

    // 向上删除父元素
    Delete(iter, std::move(parent), parent_slot_id);
}

void BTree::Merge(LeafNode&& left, LeafNode&& right) {
    for (uint16_t i = 0; i < right.count(); i++) {
        bool success = left.Append(right.GetKey(i), right.GetValue(i));
        assert(success);
    }
    right.Destroy();
    bucket_->pager().Free(right.page_id(), 1);
}

void BTree::Delete(Iterator* iter) {
    auto [pgid, pos] = iter->Front();
    LeafNode node{ this, pgid, true };
    node.Delete(pos);
    if (node.GetFillRate() >= 0.4) {
        return;
    }

    iter->Pop();
    if (iter->Empty()) {
        // 如果没有父节点
        // 是叶子节点就跳过
        return;
    }

    auto [parent, parent_slot_id, sibling_id, left_sibling] = GetSibling(iter);
    if (left_sibling) --parent_slot_id;

    LeafNode sibling{ this, sibling_id, true };
    if (bucket_->tx().IsLegacyTx(sibling.last_modified_txid())) {
        sibling = LeafNode{ this, sibling.Copy().Release() };
        if (left_sibling) {
            parent.SetLeftChild(parent_slot_id, sibling.page_id());
        } else {
            parent.SetLeftChild(parent_slot_id + 1, sibling.page_id());
        }
    }

    // 兄弟节点填充率充足
    if (sibling.GetFillRate() > 0.5 && sibling.count() >= 2) {
        std::span<const uint8_t> new_key;
        if (left_sibling) {
            // 左兄弟节点的末尾的元素插入到当前节点的头部
            // 更新父元素key为当前节点的新首元素key
            SlotId tail_slot_id = sibling.count() - 1;
            auto success = node.Insert(0, sibling.GetKey(tail_slot_id), sibling.GetValue(tail_slot_id));
            assert(success);
            sibling.Pop();
            new_key = node.GetKey(0);
        } else {
            // 右兄弟节点的头部的元素插入到当前节点的尾部
            // 更新父元素key为右兄弟的新首元素
            auto success = node.Append(sibling.GetKey(0), sibling.GetValue(0));
            assert(success);
            sibling.Delete(0);
            new_key = sibling.GetKey(0);
        }
        bool success = parent.Update(parent_slot_id, new_key, parent.GetLeftChild(parent_slot_id), false);
        //bool success = false;
        if (success == false) {
            // 父节点内的空间不足以更新key，删除父节点对应的元素，将其转换为向上插入
            parent.Delete(parent_slot_id, true);
            iter->Push({ parent.page_id(), parent_slot_id });
            if (left_sibling) {
                Put(iter, std::move(sibling), std::move(node), new_key, false);
            } else {
                Put(iter, std::move(node), std::move(sibling), new_key, false);
            }
        }
        return;
    }

    // 合并
    if (left_sibling) {
        Merge(std::move(sibling), std::move(node));
    }
    else {
        Merge(std::move(node), std::move(sibling));
    }

    // 向上删除父元素
    Delete(iter, std::move(parent), parent_slot_id);
}

std::tuple<std::span<const uint8_t>, BranchNode> BTree::Split(BranchNode* left, SlotId insert_slot_id, std::span<const uint8_t> insert_key, PageId insert_right_child) {
    assert(insert_slot_id <= left->count());
    
    BranchNode right{ this, bucket_->pager().Alloc(1), true };
    right.Build(kPageInvalidId);

    auto saved_left_count = left->count();
    assert(saved_left_count >= 2);
    
    PageId insert_left_child = left->GetLeftChild(insert_slot_id);
    left->SetLeftChild(insert_slot_id, insert_right_child);

    for (intptr_t i = saved_left_count - 1; i >= 0; --i) {
        auto success = right.Append(left->GetKey(i), left->GetLeftChild(i), false);
        assert(success);
        left->Pop(false);
        if (left->GetFillRate() <= 0.5 || right.GetFillRate() >= 0.5) {
            break;
        }
    }
    std::reverse(right.slots(), right.slots() + right.count());
    right.SetTailChild(left->GetTailChild());

    if (insert_slot_id > left->count()) {
        auto success = right.Insert(insert_slot_id - left->count(), insert_key, insert_left_child, false);
        assert(success);
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
        if (!success) {
            // 若插入到左侧失败，检查是否是插入到末尾
            if (insert_slot_id == left->count()) {
                // 是的话我们直接插入到右侧即可
                success = right.Insert(0, insert_key, insert_left_child, false);
                assert(success);
            } else {
                // 不是的话就移动末尾元素插入到右侧并重试
                right.Insert(0, left->GetKey(left->count() - 1), left->GetLeftChild(left->count() - 1), false);
                left->Pop(true);
                success = left->Insert(insert_slot_id, insert_key, insert_left_child, false);
                assert(success);
            }
        }
    }
    
    std::span<const uint8_t> up_key;
    if (left->count() >= 2) {
        assert(right.count() >= 1);
        // 左侧末尾元素上升
        up_key = left->GetKey(left->count() - 1);
        left->Pop(true);
        
    } else {
        assert(right.count() >= 2);
        // 右侧首元素上升
        up_key = right.GetKey(0);
        auto left_child = right.GetLeftChild(0);
        right.Delete(0, false);
        left->SetTailChild(left_child);
    }
    return { up_key, std::move(right) };
}

void BTree::Put(Iterator* iter, Node&& left, Node&& right, std::span<const uint8_t> key, bool branch_put) {
    if (iter->Empty()) {
        root_pgid_ = bucket_->pager().Alloc(1);
        BranchNode node{ this, root_pgid_, true };
        node.Build(left.page_id());
        node.Append(key, right.page_id(), true);
        return;
    }

    auto [pgid, slot_id] = iter->Front();
    BranchNode node{ this, pgid, true };

    //      3       7
    // 1 2     3 4     7 8

    //      3       5    7 
    // 1 2     3 4     5    7 8
    if (node.Insert(slot_id, key, right.page_id(), true)) {
        return;
    }

    auto [branch_key, branch_right] = Split(&node, slot_id, key, right.page_id());
    iter->Pop();
    Put(iter, std::move(node), std::move(branch_right), branch_key, true);
}

LeafNode BTree::Split(LeafNode* left, SlotId insert_slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    assert(insert_slot_id <= left->count());

    LeafNode right{ this, bucket_->pager().Alloc(1), false };
    right.Build();

    auto saved_left_count = left->count();
    assert(saved_left_count >= 2);

    for (intptr_t i = saved_left_count - 1; i >= 0; --i) {
        auto success = right.Append(left->GetKey(i), left->GetValue(i));
        assert(success);
        left->Pop();
        if (left->GetFillRate() <= 0.5 || right.GetFillRate() >= 0.5) {
            break;
        }
    }
    std::reverse(right.slots(), right.slots() + right.count());

    // 节点的填充率>50%，则可能插入失败
    if (insert_slot_id > left->count()) {
        // 失败则将首元素移动到左侧并重试
        assert(insert_slot_id != left->count());
        auto success = right.Insert(insert_slot_id - left->count(), key, value);
        assert(success);
        if (!success) {
            success = left->Append(right.GetKey(0), right.GetValue(0));
            assert(success);
            right.Delete(0);
            success = right.Insert(insert_slot_id - left->count(), key, value);
            assert(success);
        }
    } else {
        auto success = left->Insert(insert_slot_id, key, value);
        assert(success);
        if (!success) {
            // 若插入到左侧失败，检查是否是插入到末尾
            if (insert_slot_id == left->count()) {
                // 是的话我们直接插入到右侧即可
                success = right.Insert(0, key, value);
                assert(success);
            } else {
                // 不是的话就移动末尾元素插入到右侧并重试
                right.Insert(0, left->GetKey(left->count() - 1), left->GetValue(left->count() - 1));
                left->Pop();
                success = left->Insert(insert_slot_id, key, value);
                assert(success);
            }
        }
        assert(success);
    }
    
    assert(left->count() >= 1 && right.count() >= 2 || left->count() >= 2 && right.count() >= 1);
    return right;
}

void BTree::Put(Iterator* iter, std::span<const uint8_t> key, std::span<const uint8_t> value, bool insert_only) {
    if (iter->Empty()) {
        root_pgid_ = bucket_->pager().Alloc(1);
        LeafNode node{ this, root_pgid_, true };
        node.Build();
        auto success = node.Insert(0, key, value); assert(success == true);
        return;
    }

    auto [pgid, slot_id] = iter->Front();
    LeafNode node{ this, pgid, true };
    if (!insert_only && iter->status() == Iterator::Status::kEq) {
        node.Update(slot_id, key, value);
        return;
    }

    if (node.Insert(slot_id, key, value)) {
        return;
    }

    // 需要分裂再向上插入
    LeafNode right = Split(&node, slot_id, key, value);

    iter->Pop();
    // 上升右节点的第一个节点
    Put(iter, std::move(node), std::move(right), right.GetKey(0));
}


} // namespace yudb