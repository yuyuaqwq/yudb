#include "btree.h"

#include "tx_impl.h"
#include "bucket_impl.h"
#include "pager.h"

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
    iter.PathCopy();
    Put(&iter, key, value, true);
}
void BTree::Put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
    auto iter = LowerBound(key);
    iter.PathCopy();
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
    iter.PathCopy();
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

    if (node.GetFillRate() >= 40) {
        return;
    }

    //assert(node.count() > 0);
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
    if (sibling.GetFillRate() > 50) {
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
    if (node.GetFillRate() >= 40) {
        return;
    }

    iter->Pop();
    if (iter->Empty()) {
        // 如果没有父节点
        // 是叶子节点就跳过
        return;
    }

    //assert(node.count() > 0);

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
    if (sibling.GetFillRate() > 50) {
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
    BranchNode right{ this, bucket_->pager().Alloc(1), true };
    right.Build(kPageInvalidId);

    uint16_t mid = left->count() / 2;
    uint16_t right_count = mid;// +(left->count() % 2);

    int insert_right = 0;
    for (SlotId i = 0; i < right_count; ++i) {
        auto left_slot_id = mid + i;
        if (insert_right == 0 && left_slot_id == insert_slot_id) {
            // 插入节点的子节点使用左侧元素的子节点，并将右子节点交换给它
            auto left_child = left->GetLeftChild(left_slot_id);
            left->SetLeftChild(left_slot_id, insert_right_child);
            right.Append(insert_key, left_child, false);
            insert_right = 1;
            --i;
            continue;
        }
        right.Append(left->GetKey(left_slot_id), left->GetLeftChild(left_slot_id), false);
    }
    for (uint16_t i = 0; i < right_count; ++i) {
        left->Pop(false);
    }

    right.SetTailChild(left->GetTailChild());
    if (insert_right == 0) {
        if (insert_slot_id == left->count() + right.count()) {
            right.Append(insert_key, right.GetTailChild(), false);
            right.SetTailChild(insert_right_child);
        }
        else {
            left->Insert(insert_slot_id, insert_key, insert_right_child, true);
        }
    }
    assert(left->count() > 1);
    assert(right.count() > 1);

    // 左侧末尾元素上升
    left->SetTailChild(left->GetLeftChild(left->count() - 1));
    auto up_key = left->GetKey(left->count() - 1);
    left->Pop(true);
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
    LeafNode right{ this, bucket_->pager().Alloc(1), false };
    right.Build();

    uint16_t mid = left->count() / 2;
    uint16_t right_count = mid;// +(left->count() % 2);

    //auto page_id = left->page_id();

    int insert_right = 0;
    for (SlotId i = 0; i < right_count; i++) {
        auto left_slot_id = mid + i;
        if (insert_right == 0 && left_slot_id == insert_slot_id) {
            right.Append(key, value);
            insert_right = 1;
            --i;
            continue;
        }
        right.Append(left->GetKey(left_slot_id), left->GetValue(left_slot_id));
    }
    for (uint16_t i = 0; i < right_count; i++) {
        left->Pop();
    }

    if (insert_right == 0) {
        if (insert_slot_id == left->count() + right.count()) {
            insert_right = 1;
            right.Append(key, value);
        } else {
            left->Insert(insert_slot_id, key, value);
        }
    }
    assert(left->count() > 1);
    assert(right.count() > 1);
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