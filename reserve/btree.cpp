#include "btree.h"

#include "tx.h"
#include "bucket.h"
#include "pager.h"

namespace yudb {

BTree::BTree(Bucket* bucket, PageId* root_pgid) :
    bucket_{ bucket },
    root_pgid_ { root_pgid } {}


BTree::Iterator BTree::LowerBound(std::span<const uint8_t> key) const {
    Iterator iter{ this };
    auto status = iter.Top(key);
    while (status == Iterator::Status::kDown) {
        status = iter.Down(key);
    }
    return iter;
}

BTree::Iterator BTree::Get(std::span<const uint8_t> key) const {
    auto iter = LowerBound(key);
    if (iter.comp_result() != Iterator::CompResult::kEq) {
        return Iterator{ this };
    }
    return iter;
}

void BTree::Put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
    auto iter = LowerBound(key);
    PathCopy(&iter);
    Put(&iter, key, value);
}

bool BTree::Delete(std::span<const uint8_t> key) {
    auto iter = LowerBound(key);
    if (iter.comp_result() != Iterator::CompResult::kEq) {
        return false;
    }
    PathCopy(&iter);
    Delete(&iter, key);
    return true;
}


BTree::Iterator BTree::begin() const noexcept {
    Iterator iter { this };
    iter.First(*root_pgid_);
    return iter;
}

BTree::Iterator BTree::end() const noexcept {
    return Iterator{ this };
}


void BTree::Print(bool str, PageId pgid, int level) const {
    std::string indent(level * 4, ' ');
    Noder noder{ this, pgid };
    auto& node = noder.node();
    if (noder.IsBranch()) {
        Print(str, node.body.tail_child, level + 1);
        for (int i = node.element_count - 1; i >= 0; i--) {
            std::string key;
            auto [buf, size, ref] = noder.SpanLoad(node.body.branch[i].key);
            if (str) {
                std::cout << std::format("{}branch[{},{}]::key::{}::level::{}\n", indent, pgid, node.free_size, std::string_view{reinterpret_cast<const char*>(buf), size}, level);
            }
            else {
                for (int j = 0; j < size; j++) {
                    key += std::format("{:02x}", buf[j]) + " ";
                }
                std::cout << std::format("{}branch[{},{}]::key::{}::level::{}\n", indent, pgid, node.free_size, key, level);
            }
            Print(str, node.body.branch[i].left_child, level + 1);
        }
        //noder.BlockPrint(); printf("\n");
    }
    else {
        assert(noder.IsLeaf());
        for (int i = node.element_count - 1; i >= 0; i--) {
            std::string key;
            auto [buf, size, ref] = noder.SpanLoad(node.body.leaf[i].key);
            if (str) {
                std::cout << std::format("{}leaf[{},{}]::key::{}::level::{}\n", indent, pgid, node.free_size, std::string_view{ reinterpret_cast<const char*>(buf), size }, level);
            }
            else {
                for (int j = 0; j < size; j++) {
                    key += std::format("{:02x}", buf[j]) + " ";
                }
                std::cout << std::format("{}leaf[{},{}]::key::{}::level::{}\n", indent, pgid, node.free_size, key, level);
            }
        }
        //noder.BlockPrint(); printf("\n");
    }
}

void BTree::Print(bool str) const {
    Print(str, *root_pgid_, 0);
}



std::tuple<Noder, uint16_t, Noder, bool> BTree::GetSibling(Iterator* iter) {
    auto [parent_pgid, parent_pos] = iter->Cur();
    Noder parent{ this, parent_pgid };
    auto& parent_node = parent.node();
        
    bool left_sibling = false;
    PageId sibling_pgid;
    if (parent_pos == parent_node.element_count) {
        // 是父节点中最大的元素，只能选择左兄弟节点
        left_sibling = true;
        sibling_pgid = parent.BranchGetLeftChild(parent_pos - 1);
    }
    else {
        sibling_pgid = parent.BranchGetRightChild(parent_pos);
    }

    Noder sibling{ this, sibling_pgid };

    iter->Pop();
    return { std::move(parent), parent_pos, std::move(sibling), left_sibling };
}


/*
* 分支节点的合并
*/
void BTree::Merge(Noder&& left, Noder&& right, Span&& down_key) {
    auto& left_node = left.node();
    auto& right_node = right.node();

    auto old_element_count = left_node.element_count;
    auto success = left.BranchAlloc(right_node.element_count + 1); assert(success);
    left_node.body.branch[old_element_count].key = std::move(down_key);
    left_node.body.branch[old_element_count].left_child = left_node.body.tail_child;
    for (uint16_t i = 0; i < right_node.element_count; i++) {
        auto key = right.SpanMove(&left, std::move(right_node.body.branch[i].key));
        left.BranchSet(i + old_element_count + 1, std::move(key), right_node.body.branch[i].left_child);
    }
    left_node.body.tail_child = right_node.body.tail_child;

    right.SpanClear();
    bucket_->pager().Free(right.page_id(), 1);
}

/*
* 分支节点的删除
*/
void BTree::Delete(Iterator* iter, Noder&& noder, uint16_t left_del_pos) {
    auto& node = noder.node();

    auto min_free_size = bucket_->pager().page_size() >> 1;
    noder.BranchDelete(left_del_pos, true);
    if (node.free_size < min_free_size) {
        return;
    }

    if (iter->Empty()) {
        // 如果没有父节点
        // 判断是否没有任何子节点了，是则变更余下最后一个子节点为根节点
        if (node.element_count == 0) {
            auto old_root = *root_pgid_;
            *root_pgid_ = node.body.tail_child;
            bucket_->pager().Free(old_root, 1);
        }
        return;
    }

    assert(node.element_count > 0);

    auto [parent, parent_pos, sibling, left_sibling] = GetSibling(iter);
    if (left_sibling) --parent_pos;
    auto& parent_node = parent.node();

    if (bucket_->tx().IsExpiredTxId(sibling.node().last_modified_txid)) {
        sibling = sibling.Copy();
        if (left_sibling) {
            parent.BranchSetLeftChild(parent_pos, sibling.page_id());
        }
        else {
            parent.BranchSetLeftChild(parent_pos + 1, sibling.page_id());
        }
    }
    auto& sibling_node = sibling.node();
    if (sibling_node.free_size < min_free_size) {
        // 若兄弟节点内元素数充足
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
            auto parent_key = parent.SpanMove(&noder, std::move(parent_node.body.branch[parent_pos].key));
            auto sibling_key = sibling.SpanMove(&parent, std::move(sibling_node.body.branch[sibling_node.element_count - 1].key));

            auto success = noder.BranchAlloc(1); assert(success);
            noder.BranchInsert(0, std::move(parent_key), sibling_node.body.tail_child, false);
            sibling.BranchDelete(sibling_node.element_count - 1, true);

            parent_node.body.branch[parent_pos].key = std::move(sibling_key);
        }
        else {
            parent.BranchSetLeftChild(parent_pos + 1, sibling.page_id());
            // 右兄弟节点的头元素上升到父节点的指定位置
            // 父节点的对应元素下降到当前节点的尾部
            // 上升元素其左子节点挂在下降的父节点元素的右侧

            //                       17
            //      12                                    27            37
            // 1 5      10 15                    20 25         30 35         40 45

            //                                27
            //      12          17                               37
            // 1 5      10 15       20 25                30 35         40 45

            auto parent_key = parent.SpanMove(&noder, std::move(parent_node.body.branch[parent_pos].key));
            auto sibling_key = sibling.SpanMove(&parent, std::move(sibling_node.body.branch[0].key));

            auto success = noder.BranchAlloc(1); assert(success);
            noder.BranchInsert(node.element_count - 1, std::move(parent_key), sibling_node.body.branch[0].left_child, true);
            sibling.BranchDelete(0, false);

            parent_node.body.branch[parent_pos].key = std::move(sibling_key);

        }
        return;
    }

    // 合并
    if (left_sibling) {
        auto down_key = parent.SpanMove(&sibling, std::move(parent_node.body.branch[parent_pos].key));
        Merge(std::move(sibling), std::move(noder), std::move(down_key));
    }
    else {
        auto down_key = parent.SpanMove(&noder, std::move(parent_node.body.branch[parent_pos].key));
        Merge(std::move(noder), std::move(sibling), std::move(down_key));
    }

    // 向上删除父元素
    Delete(iter, std::move(parent), parent_pos);
}

/*
* 叶子节点的合并
*/
void BTree::Merge(Noder&& left, Noder&& right) {
    auto& left_node = left.node();
    auto& right_node = right.node();
    auto old_element_count = left_node.element_count;
    auto success = left.LeafAlloc(right_node.element_count); assert(success);
    for (uint16_t i = 0; i < right_node.element_count; i++) {
        auto key = right.SpanMove(&left, std::move(right_node.body.leaf[i].key));
        auto value = right.SpanMove(&left, std::move(right_node.body.leaf[i].value));
        left.LeafSet(i + old_element_count, std::move(key), std::move(value));
    }

    right.SpanClear();
    bucket_->pager().Free(right.page_id(), 1);
}

/*
* 叶子节点的删除
*/
void BTree::Delete(Iterator* iter, std::span<const uint8_t> key) {
    auto [pgid, pos] = iter->Cur();
    Noder noder{ this, pgid };
    auto& node = noder.node();

    auto min_free_size = bucket_->pager().page_size() >> 1;
    noder.LeafDelete(pos);
    if (node.free_size < min_free_size) {
        return;
    }

    iter->Pop();
    if (iter->Empty()) {
        // 如果没有父节点
        // 是叶子节点就跳过
        return;
    }

    assert(node.element_count > 1);

    auto [parent, parent_pos, sibling, left_sibling] = GetSibling(iter);
    if (left_sibling) --parent_pos;
    auto& parent_node = parent.node();

    if (bucket_->tx().IsExpiredTxId(sibling.node().last_modified_txid)) {
        sibling = sibling.Copy();
        if (left_sibling) {
            parent.BranchSetLeftChild(parent_pos, sibling.page_id());
        }
        else {
            parent.BranchSetLeftChild(parent_pos + 1, sibling.page_id());
        }
    }
    auto& sibling_node = sibling.node();
    if (sibling_node.free_size < min_free_size) {
        // 若兄弟节点内元素数充足
        Span new_key;
        if (left_sibling) {
            // 左兄弟节点的末尾的元素插入到当前节点的头部
            // 更新父元素key为当前节点的新首元素key
            auto key = sibling.SpanMove(&noder, std::move(sibling_node.body.leaf[sibling_node.element_count - 1].key));
            auto value = sibling.SpanMove(&noder, std::move(sibling_node.body.leaf[sibling_node.element_count - 1].value));
            sibling.LeafDelete(sibling_node.element_count - 1);

            new_key = noder.SpanCopy(&parent, key);
            auto success = noder.LeafAlloc(1); assert(success);
            noder.LeafInsert(0, std::move(key), std::move(value));
        }
        else {
            // 右兄弟节点的头部的元素插入到当前节点的尾部
            // 更新父元素key为右兄弟的新首元素
            auto key = sibling.SpanMove(&noder, std::move(sibling_node.body.leaf[0].key));
            auto value = sibling.SpanMove(&noder, std::move(sibling_node.body.leaf[0].value));
            sibling.LeafDelete(0);

            new_key = sibling.SpanCopy(&parent, sibling_node.body.leaf[0].key);
            auto success = noder.LeafAlloc(1); assert(success);
            noder.LeafInsert(node.element_count - 1, std::move(key), std::move(value));
        }
        parent.SpanFree(std::move(parent_node.body.branch[parent_pos].key));
        parent_node.body.branch[parent_pos].key = std::move(new_key);
        return;
    }

    // 合并
    if (left_sibling) {
        Merge(std::move(sibling), std::move(noder));
    }
    else {
        Merge(std::move(noder), std::move(sibling));
    }

    // 向上删除父元素
    Delete(iter, std::move(parent), parent_pos);
}


/*
* 分支节点的分裂
* 返回左侧节点中末尾上升的元素，新右节点
*/
std::tuple<Span, Noder> BTree::Split(Noder* left, uint16_t insert_pos, Span&& insert_key, PageId insert_right_child) {
    Noder right{ this, bucket_->pager().Alloc(1) };
    right.BranchBuild();

    auto& left_node = left->node();
    auto& right_node = right.node();

    int insert_right = 0;
    auto min_free_size = bucket_->pager().page_size() >> 1;
    PageId insert_left_child = kPageInvalidId;
    for (uint16_t i = 0; right_node.free_size > min_free_size; i++) {
        auto left_pos = left_node.element_count - i - 1;
        if (insert_right == 0 && i == left_node.element_count - insert_pos) {
            if (insert_left_child == kPageInvalidId) {
                insert_left_child = left_node.body.tail_child;
                left_node.body.tail_child = insert_right_child;
            }
            right.BranchAlloc(1);
            right.BranchSet(i,
                std::move(insert_key),
                insert_left_child
            );
            insert_right = 1;
            --i;
            continue;
        }
        right.BranchAlloc(1);
        PageId left_child;
        if (left_pos == insert_pos) {
            // 当前节点的左子节点指向了孩子的分裂前的节点，使其指向分裂出来的的右节点
            // 孩子的分裂前的节点移交给左侧邻居
            // insert_left_child是插入到右节点用的
            insert_left_child = left_node.body.branch[left_pos].left_child;
            left_child = insert_right_child;
            // insert_left_child是插入到左节点用的
            insert_right_child = insert_left_child;
        }
        else {
            left_child = left_node.body.branch[left_pos].left_child;
        }
        right.BranchSet(i + insert_right,
            left->SpanMove(&right, std::move(left_node.body.branch[left_pos].key)),
            left_child
        );
    }
    for (int i = 0; i < right_node.element_count / 2; ++i) {
        std::swap(right_node.body.branch[i], right_node.body.branch[right_node.element_count - i - 1]);
    }
    right_node.body.tail_child = left_node.body.tail_child;

    assert(left_node.free_size <= min_free_size);
    // 先保证left余下元素及子节点关系的正确性
    left_node.body.tail_child = left_node.body.branch[left_node.element_count - right_node.element_count].left_child;
    left->BranchFree(right_node.element_count - insert_right);
    if (insert_right == 0) {
        // 需要注意的是，insert_right_child可能会与left_node.body.tail_child相同
        // 即对应将元素插入到左侧末尾的时候，但是无关紧要
        // 下面会上升末尾的元素，tail_child的修正结果也不会变
        left->BranchAlloc(1);
        left->BranchInsert(insert_pos, std::move(insert_key), insert_right_child, true);
    }

    assert(left_node.element_count > 2);
    assert(right_node.element_count > 2);

    // 左侧末尾元素上升，其left_child变为tail_child
    Span span{ std::move(left_node.body.branch[left_node.element_count - 1].key) };
    left_node.body.tail_child = left_node.body.branch[left_node.element_count - 1].left_child;
    left->BranchFree(1);

    return { std::move(span), std::move(right) };
}

/*
* 分支节点的插入
*/
void BTree::Put(Iterator* iter, Noder&& left_child, Noder&& right_child, Span* key, bool from_branch) {
    if (iter->Empty()) {
        *root_pgid_ = bucket_->pager().Alloc(1);
        Noder noder{ this, *root_pgid_ };

        noder.BranchBuild();
        noder.node().body.tail_child = left_child.page_id();

        Span noder_key_span;
        if (from_branch) {
            noder_key_span = left_child.SpanMove(&noder, std::move(*key));
        } else {
            noder_key_span = right_child.SpanCopy(&noder, *key);
        }
        auto success = noder.BranchAlloc(1); assert(success);
        noder.BranchInsert(0, std::move(noder_key_span), right_child.page_id(), true);
        return;
    }

    auto [pgid, pos] = iter->Cur();
    Noder noder{ this, pgid };
    //      3       7
    // 1 2     3 4     7 8

    //      3       5    7 
    // 1 2     3 4     5    7 8

    Span noder_key_span;
    if (from_branch) {
        noder_key_span = left_child.SpanMove(&noder, std::move(*key));
    }
    else {
        noder_key_span = right_child.SpanCopy(&noder, *key);
    }

    if (noder.BranchAlloc(1)) {
        noder.BranchInsert(pos, std::move(noder_key_span), right_child.page_id(), true);
        return;
    }

    auto [branch_key, branch_right] = Split(&noder, pos, std::move(noder_key_span), right_child.page_id());
    iter->Pop();
    Put(iter, std::move(noder), std::move(branch_right), &branch_key, true);
}

/*
* 叶子节点的分裂
* 返回新右节点
*/
Noder BTree::Split(Noder* left, uint16_t insert_pos, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    Noder right{ this, bucket_->pager().Alloc(1) };
    right.LeafBuild();

    auto& left_node = left->node();
    auto& right_node = right.node();

    int insert_right = 0;
    auto min_free_size = bucket_->pager().page_size() >> 1;
    for (uint16_t i = 0; right_node.free_size > min_free_size; i++) {
        auto left_pos = left_node.element_count - i - 1;
        if (insert_right == 0 && i == left_node.element_count - insert_pos) {
            auto key_span = right.SpanAlloc(key);
            auto value_span = right.SpanAlloc(value);
            right.LeafAlloc(1);
            right.LeafSet(i, std::move(key_span), std::move(value_span));
            insert_right = 1;
            --i;
            continue;
        }
        right.LeafAlloc(1);
        right.LeafSet(i + insert_right,
            left->SpanMove(&right, std::move(left_node.body.leaf[left_pos].key)),
            left->SpanMove(&right, std::move(left_node.body.leaf[left_pos].value))
        );
    }
    for (int i = 0; i < right_node.element_count / 2; ++i) {
        std::swap(right_node.body.leaf[i], right_node.body.leaf[right_node.element_count - i - 1]);
    }

    left->LeafFree(right_node.element_count - insert_right);
    if (insert_right == 0) {
        auto key_span = left->SpanAlloc(key);
        auto value_span = left->SpanAlloc(value);
        left->LeafAlloc(1);
        left->LeafInsert(insert_pos, std::move(key_span), std::move(value_span));
    }
    assert(left_node.element_count > 2);
    assert(right_node.element_count > 2);
    return right;
}

/*
* 叶子节点的插入
*/
void BTree::Put(Iterator* iter, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    if (iter->Empty()) {
        *root_pgid_ = bucket_->pager().Alloc(1);
        Noder noder{ this, *root_pgid_ };
        noder.LeafBuild();
        auto success = noder.LeafAlloc(1); assert(success);
        auto key_span = noder.SpanAlloc(key);
        auto value_span = noder.SpanAlloc(value);
        noder.LeafInsert(0, std::move(key_span), std::move(value_span));
        return;
    }

    auto [pgid, pos] = iter->Cur();
    Noder noder{ this, pgid };
    auto& node = noder.node();

    if (iter->comp_result() == Iterator::CompResult::kEq) {
        noder.SpanFree(std::move(node.body.leaf[pos].value));
        auto value_span = noder.SpanAlloc(value);
        node.body.leaf[pos].value = std::move(value_span);
        return;
    }

    if (noder.LeafAlloc(1)) {
        auto key_span = noder.SpanAlloc(key);
        auto value_span = noder.SpanAlloc(value);
        noder.LeafInsert(pos, std::move(key_span), std::move(value_span));
        return;
    }

    // 空间不足，需要分裂再向上插入
    Noder right = Split(&noder, pos, key, value);

    iter->Pop();
    // 上升右节点的第一个节点
    Put(iter, std::move(noder),
        std::move(right),
        &right.node().body.leaf[0].key
    );
}


void BTree::PathCopy(Iterator* iter) {
    if (iter->Empty()) {
        return;
    }
    auto& tx = bucket_->tx();
    auto lower_pgid = kPageInvalidId;
    for (ptrdiff_t i = iter->Size() - 1; i >= 0; i--) {
        auto& [pgid, index] = iter->Index(i);
        Noder noder{ this, pgid };
        if (!tx.IsExpiredTxId(noder.node().last_modified_txid)) {
            if (noder.IsBranch()) {
                assert(lower_pgid != kPageInvalidId);
                noder.BranchSetLeftChild(index, lower_pgid);
            }
            return;
        }

        Noder new_noder = noder.Copy();
        new_noder.node().last_modified_txid = tx.txid();
        if (new_noder.IsBranch()) {
            assert(lower_pgid != kPageInvalidId);
            new_noder.BranchSetLeftChild(index, lower_pgid);
        }
        lower_pgid = new_noder.page_id();
        pgid = lower_pgid;
    }
    *iter->btree_->root_pgid_ = lower_pgid;
}


} // namespace yudb