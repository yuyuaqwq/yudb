#include "btree.h"

#include "tx.h"
#include "bucket.h"
#include "pager.h"

namespace yudb {

BTree::BTree(Bucket* bucket, PageId& root_pgid) :
    bucket_{ bucket },
    root_pgid_ { root_pgid }
{
    auto free_size = pager()->page_size() - (sizeof(Node) - sizeof(Node::body));

    max_leaf_element_count_ = free_size / sizeof(Node::LeafElement);
    max_branch_element_count_ = (free_size - sizeof(PageId)) / sizeof(Node::BranchElement);
}


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
    iter.First(root_pgid_);
    return iter;
}

BTree::Iterator BTree::end() const noexcept {
    return Iterator{ this };
}


Pager* BTree::pager() const { return bucket_->pager(); }

Tx* BTree::tx() const { return bucket_->tx(); }

UpdateTx* BTree::update_tx() const{ return bucket_->update_tx(); }


void BTree::Print(bool str, PageId pgid, int level) const {
    std::string indent(level * 4, ' ');
    Noder noder{ this, pgid };
    auto node = noder.node();
    if (noder.IsBranch()) {
        Print(str, node->body.tail_child, level + 1);
        for (int i = node->element_count - 1; i >= 0; i--) {
            std::string key;
            auto [buf, size, ref] = noder.SpanLoad(node->body.branch[i].key);
            if (str) {
                std::cout << std::format("{}branch[{}]::key::{}::level::{}\n", indent, pgid, std::string_view{ reinterpret_cast<const char*>(buf), size }, level);
            }
            else {
                for (int j = 0; j < size; j++) {
                    key += std::format("{:02x}", buf[j]) + " ";
                }
                std::cout << std::format("{}branch[{}]::key::{}::level::{}\n", indent, pgid, key, level);
            }
            Print(str, node->body.branch[i].left_child, level + 1);
        }
        //noder.OverflowPrint(); printf("\n");
    }
    else {
        assert(noder.IsLeaf());
        for (int i = node->element_count - 1; i >= 0; i--) {
            std::string key;
            auto [buf, size, ref] = noder.SpanLoad(node->body.leaf[i].key);
            if (str) {
                std::cout << std::format("{}leaf[{}]::key::{}::level::{}\n", indent, pgid, std::string_view{ reinterpret_cast<const char*>(buf), size }, level);
            }
            else {
                for (int j = 0; j < size; j++) {
                    key += std::format("{:02x}", buf[j]) + " ";
                }
                std::cout << std::format("{}leaf[{}]::key::{}::level::{}\n", indent, pgid, key, level);
            }
        }
        //noder.OverflowPrint(); printf("\n");
    }
}

void BTree::Print(bool str) const {
    Print(str, root_pgid_, 0);
}



std::tuple<Noder, uint16_t, Noder, bool> BTree::GetSibling(Iterator* iter) {
    auto [parent_pgid, parent_pos] = iter->Cur();
    Noder parent{ this, parent_pgid };
    auto parent_node = parent.node();
        
    bool left_sibling = false;
    PageId sibling_pgid;
    if (parent_pos == parent_node->element_count) {
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
    auto left_node = left.node();
    auto right_node = right.node();
    left_node->body.branch[left_node->element_count].key = std::move(down_key);
    left_node->body.branch[left_node->element_count].left_child = left_node->body.tail_child;
    ++left_node->element_count;
    for (uint16_t i = 0; i < right_node->element_count; i++) {
        auto key = right.SpanMove(&left, std::move(right_node->body.branch[i].key));
        left.BranchSet(i + left_node->element_count, std::move(key), right_node->body.branch[i].left_child);
    }
    left_node->body.tail_child = right_node->body.tail_child;
    left_node->element_count += right_node->element_count;

    right.SpanClear();
    pager()->Free(right.page_id(), 1);
}

/*
* 分支节点的删除
*/
void BTree::Delete(Iterator* iter, Noder&& noder, uint16_t left_del_pos) {
    auto node = noder.node();

    noder.BranchDelete(left_del_pos, true);
    if (node->element_count >= (max_branch_element_count_ >> 1)) {
        return;
    }

    if (iter->Empty()) {
        // 如果没有父节点
        // 判断是否没有任何子节点了，是则变更余下最后一个子节点为根节点
        if (node->element_count == 0) {
            auto old_root = root_pgid_;
            root_pgid_ = node->body.tail_child;
            pager()->Free(old_root, 1);
        }
        return;
    }

    assert(node->element_count > 0);

    auto [parent, parent_pos, sibling, left_sibling] = GetSibling(iter);
    if (left_sibling) --parent_pos;
    auto parent_node = parent.node();
    if (sibling.node()->element_count > (max_leaf_element_count_ >> 1)) {
        // 若兄弟节点内元素数充足
        if (bucket_->update_tx()->IsExpiredTxId(sibling.node()->last_modified_txid)) {
            sibling = sibling.Copy();
        }
        auto sibling_node = sibling.node();
        if (left_sibling) {
            parent.BranchSetLeftChild(parent_pos, sibling.page_id());
            // 左兄弟节点的末尾元素上升到父节点指定位置
            // 父节点的对应元素下降到当前节点的头部
            // 上升元素其右子节点挂在下降的父节点元素的左侧

            //                                27
            //      12          17                               37
            // 1 5      10 15       20 25                30 35         40 45

            //                       17
            //      12                                    27            37
            // 1 5      10 15                    20 25         30 35         40 45
            auto parent_key = parent.SpanMove(&noder, std::move(parent_node->body.branch[parent_pos].key));
            auto sibling_key = sibling.SpanMove(&parent, std::move(sibling_node->body.branch[sibling_node->element_count - 1].key));

            noder.BranchInsert(0, std::move(parent_key), sibling_node->body.tail_child, false);
            sibling.BranchDelete(sibling_node->element_count - 1, true);

            parent_node->body.branch[parent_pos].key = std::move(sibling_key);
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

            auto parent_key = parent.SpanMove(&noder, std::move(parent_node->body.branch[parent_pos].key));
            auto sibling_key = sibling.SpanMove(&parent, std::move(sibling_node->body.branch[0].key));

            noder.BranchInsert(node->element_count, std::move(parent_key), sibling_node->body.branch[0].left_child, true);
            sibling.BranchDelete(0, false);

            parent_node->body.branch[parent_pos].key = std::move(sibling_key);

        }
        return;
    }

    // 合并
    if (left_sibling) {
        auto down_key = parent.SpanMove(&sibling, std::move(parent_node->body.branch[parent_pos].key));
        Merge(std::move(sibling), std::move(noder), std::move(down_key));
    }
    else {
        auto down_key = parent.SpanMove(&noder, std::move(parent_node->body.branch[parent_pos].key));
        Merge(std::move(noder), std::move(sibling), std::move(down_key));
    }

    // 向上删除父元素
    Delete(iter, std::move(parent), parent_pos);
}

/*
* 叶子节点的合并
*/
void BTree::Merge(Noder&& left, Noder&& right) {
    auto left_node = left.node();
    auto right_node = right.node();
    for (uint16_t i = 0; i < right_node->element_count; i++) {
        auto key = right.SpanMove(&left, std::move(right_node->body.leaf[i].key));
        auto value = right.SpanMove(&left, std::move(right_node->body.leaf[i].value));

        left.LeafSet(i + left_node->element_count, std::move(key), std::move(value));
    }
    left_node->element_count += right_node->element_count;

    right.SpanClear();
    pager()->Free(right.page_id(), 1);
}

/*
* 叶子节点的删除
*/
void BTree::Delete(Iterator* iter, std::span<const uint8_t> key) {
    auto [pgid, pos] = iter->Cur();
    Noder noder{ this, pgid };
    auto node = noder.node();

    noder.LeafDelete(pos);
    if (node->element_count >= (max_leaf_element_count_ >> 1)) {
        return;
    }

    iter->Pop();
    if (iter->Empty()) {
        // 如果没有父节点
        // 是叶子节点就跳过
        return;
    }

    assert(node->element_count > 1);

    auto [parent, parent_pos, sibling, left_sibling] = GetSibling(iter);
    if (left_sibling) --parent_pos;
    auto parent_node = parent.node();

    if (sibling.node()->element_count > (max_leaf_element_count_ >> 1)) {
        // 若兄弟节点内元素数充足
        if (bucket_->update_tx()->IsExpiredTxId(sibling.node()->last_modified_txid)) {
            sibling = sibling.Copy();
        }
        auto sibling_node = sibling.node();
        Span new_key;
        if (left_sibling) {
            parent.BranchSetLeftChild(parent_pos, sibling.page_id());
            // 左兄弟节点的末尾的元素插入到当前节点的头部
            // 更新父元素key为当前节点的新首元素key
            auto key = sibling.SpanMove(&noder, std::move(sibling_node->body.leaf[sibling_node->element_count - 1].key));
            auto value = sibling.SpanMove(&noder, std::move(sibling_node->body.leaf[sibling_node->element_count - 1].value));
            sibling.LeafDelete(sibling_node->element_count - 1);

            new_key = noder.SpanCopy(&parent, key);
            noder.LeafInsert(0, std::move(key), std::move(value));
        }
        else {
            parent.BranchSetLeftChild(parent_pos + 1, sibling.page_id());
            // 右兄弟节点的头部的元素插入到当前节点的尾部
            // 更新父元素key为右兄弟的新首元素
            auto key = sibling.SpanMove(&noder, std::move(sibling_node->body.leaf[0].key));
            auto value = sibling.SpanMove(&noder, std::move(sibling_node->body.leaf[0].value));
            sibling.LeafDelete(0);

            new_key = sibling.SpanCopy(&parent, sibling_node->body.leaf[0].key);
            noder.LeafInsert(node->element_count, std::move(key), std::move(value));
        }
        parent.SpanFree(std::move(parent_node->body.branch[parent_pos].key));
        parent_node->body.branch[parent_pos].key = new_key;
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
    Noder right{ this, pager()->Alloc(1) };
    right.BranchBuild();

    auto left_node = left->node();
    auto right_node = right.node();

    uint16_t mid = left_node->element_count / 2;
    uint16_t right_count = mid + (left_node->element_count % 2);

    int insert_right = 0;
    for (uint16_t i = 0; i < right_count; i++) {
        auto left_pos = mid + i;
        if (insert_right == 0 && left_pos == insert_pos) {
            auto next_left_child = left_node->body.branch[left_pos].left_child;
            left_node->body.branch[left_pos].left_child = insert_right_child;
            right.BranchSet(i,
                left->SpanMove(&right, std::move(insert_key)),
                next_left_child
            );
            insert_right = 1;
            --i;
            continue;
        }
        auto key = left->SpanMove(&right, std::move(left_node->body.branch[left_pos].key));
        right.BranchSet(i + insert_right,
            std::move(key),
            left_node->body.branch[left_pos].left_child
        );
    }

    left_node->element_count -= right_count;
    assert(left_node->element_count > 2);
    right_node->element_count = right_count + insert_right;

    right_node->body.tail_child = left_node->body.tail_child;
    if (insert_right == 0) {
        if (insert_pos == max_leaf_element_count_) {
            right.BranchSet(right_node->element_count++, left->SpanMove(&right, std::move(insert_key)), right_node->body.tail_child);
            right_node->body.tail_child = insert_right_child;
        }
        else {
            left->BranchInsert(insert_pos, std::move(insert_key), insert_right_child, true);
        }
    }

    assert(right_node->element_count > 2);

    // 左侧末尾元素上升，其left_child变为tail_child
    Span span = left_node->body.branch[--left_node->element_count].key;
    left_node->body.tail_child = left_node->body.branch[left_node->element_count].left_child;

    return { span, std::move(right) };
}

/*
* 分支节点的插入
*/
void BTree::Put(Iterator* iter, Noder&& left, Noder&& right, Span* key, bool branch_put) {
    if (iter->Empty()) {
        root_pgid_ = pager()->Alloc(1);
        Noder noder{ this, root_pgid_ };

        noder.BranchBuild();
        noder.node()->body.tail_child = left.page_id();

        Span noder_key_span;
        if (branch_put) {
            noder_key_span = left.SpanMove(&noder, std::move(*key));
        }
        else {
            noder_key_span = right.SpanCopy(&noder, *key);
        }
        noder.BranchInsert(0, std::move(noder_key_span), right.page_id(), true);
        return;
    }

    auto [pgid, pos] = iter->Cur();
    Noder noder{ this, pgid };

    Span noder_key_span;
    if (branch_put) {
        noder_key_span = left.SpanMove(&noder, std::move(*key));
    }
    else {
        noder_key_span = right.SpanCopy(&noder, *key);
    }

    //      3       7
    // 1 2     3 4     7 8

    //      3       5    7 
    // 1 2     3 4     5    7 8
    if (noder.node()->element_count < max_leaf_element_count_) {
        noder.BranchInsert(pos, std::move(noder_key_span), right.page_id(), true);
        return;
    }

    auto [branch_key, branch_right] = Split(&noder, pos, std::move(noder_key_span), right.page_id());
    iter->Pop();
    Put(iter, std::move(noder), std::move(branch_right), &branch_key, true);
}

/*
* 叶子节点的分裂
* 返回新右节点
*/
Noder BTree::Split(Noder* left, uint16_t insert_pos, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    Noder right{ this, pager()->Alloc(1) };
    right.LeafBuild();

    auto left_node = left->node();
    auto right_node = right.node();

    uint16_t mid = left_node->element_count / 2;
    uint16_t right_count = mid + (left_node->element_count % 2);

    int insert_right = 0;
    for (uint16_t i = 0; i < right_count; i++) {
        auto left_pos = mid + i;
        if (insert_right == 0 && left_pos == insert_pos) {
            right.LeafSet(i,
                right.SpanAlloc(key),
                right.SpanAlloc(value)
            );
            insert_right = 1;
            --i;
            continue;
        }
        auto key = left->SpanMove(&right, std::move(left_node->body.leaf[left_pos].key));
        auto value = left->SpanMove(&right, std::move(left_node->body.leaf[left_pos].value));

        right.LeafSet(i + insert_right, std::move(key), std::move(value));
    }

    left_node->element_count -= right_count;
    assert(left_node->element_count > 2);
    if (insert_right == 0) {
        if (insert_pos == max_leaf_element_count_) {
            insert_right = 1;
            right.LeafSet(right_count, right.SpanAlloc(key), right.SpanAlloc(value));
        }
        else {
            left->LeafInsert(insert_pos, left->SpanAlloc(key), left->SpanAlloc(value));
        }
    }
    right_node->element_count = right_count + insert_right;
    assert(right_node->element_count > 2);

    return right;
}

/*
* 叶子节点的插入
*/
void BTree::Put(Iterator* iter, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    if (iter->Empty()) {
        root_pgid_ = pager()->Alloc(1);
        Noder noder{ this, root_pgid_ };
        noder.LeafBuild();
        noder.LeafInsert(0, noder.SpanAlloc(key), noder.SpanAlloc(value));
        return;
    }

    auto [pgid, pos] = iter->Cur();
    Noder noder{ this, pgid };
    if (iter->comp_result() == Iterator::CompResult::kEq) {
        noder.LeafSet(pos, noder.SpanAlloc(key), noder.SpanAlloc(value));
        return;
    }

    if (noder.node()->element_count < max_leaf_element_count_) {
        noder.LeafInsert(pos, noder.SpanAlloc(key), noder.SpanAlloc(key));
        return;
    }

    // 需要分裂再向上插入
    Noder right = Split(&noder, pos, key, value);

    iter->Pop();
    // 上升右节点的第一个节点
    Put(iter, std::move(noder),
        std::move(right),
        &right.node()->body.leaf[0].key
    );
}


void BTree::PathCopy(Iterator* iter) {
    if (iter->Empty()) {
        return;
    }
    auto lower_pgid = kPageInvalidId;

    for (ptrdiff_t i = iter->Size() - 1; i >= 0; i--) {
        auto& [pgid, index] = iter->Index(i);
        Noder noder{ this, pgid };
        if (!bucket_->update_tx()->IsExpiredTxId(noder.node()->last_modified_txid)) {
            return;
        }

        Noder new_noder = noder.Copy();
        new_noder.node()->last_modified_txid = bucket_->tx()->txid();
        if (new_noder.IsBranch()) {
            assert(lower_pgid != kPageInvalidId);
            new_noder.BranchSetLeftChild(index, lower_pgid);
        }
        lower_pgid = new_noder.page_id();
        pgid = lower_pgid;
    }
    iter->btree_->root_pgid_ = lower_pgid;
}


} // namespace yudb