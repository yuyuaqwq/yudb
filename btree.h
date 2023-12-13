#pragma once

#include <span>
#include <algorithm>
#include <iostream>
#include <format>
#include <string>

#include "stack.h"
#include "page.h"
#include "noder.h"
#include "pager.h"

#undef min

namespace yudb {

/*
* b+tree
*/
class BTree {
public:
    class Iterator {
    public:
        typedef enum class Status {
            kDown,
            kNext,
            kEnd,
        };

        typedef enum class CompResults {
            kEq,
            kNe,
        };

    public:
        Iterator(BTree* btree) : btree_{ btree } {}

        Status Top(std::span<const uint8_t> key) {
            comp_results_ = CompResults::kNe;
            return Down(key);
        }

        Status Down(std::span<const uint8_t> key) {
            PageId pgid;
            if (stack_.empty()) {
                pgid = btree_->root_pgid_;
                if (pgid == kPageInvalidId) {
                    return Status::kEnd;
                }
            }
            else {
                auto [parent_pgid, pos] = stack_.front();
                Noder parent{ btree_, parent_pgid };
                auto parent_node = parent.node();
                if (parent.IsLeaf()) {
                    return Status::kEnd;
                }
                pgid = parent.BranchGetLeftChild(pos);
            }
            Noder noder{ btree_, pgid };
            auto node = noder.node();

            // 在节点中进行二分查找
            uint16_t index;
            if (node->element_count > 0) {
                comp_results_ = CompResults::kNe;
                auto iter = std::lower_bound(noder.begin(), noder.end(), key, [&](const Span& span, std::span<const uint8_t> search_key) -> bool {
                    auto [buf, size] = noder.SpanLoad(span);
                    auto res = memcmp(buf, search_key.data(), std::min(size, search_key.size()));
                    if (res == 0 && size != search_key.size()) {
                        return size < search_key.size();
                    }
                    if (res != 0) {
                        return res < 0;
                    }
                    else {
                        comp_results_ = CompResults::kEq;
                        return false;
                    }
                });
                index = iter.index();
                if (comp_results_ == CompResults::kEq && noder.IsBranch()) {
                    ++index;
                }
            }
            else {
                index = 0;
            }
            stack_.push_back(std::pair{ pgid, index });
            return Status::kDown;
        }

        std::pair<PageId, uint16_t> Cur() {
            return stack_.front();
        }

        void Pop() {
            stack_.pop_back();
        }

        bool Empty() {
            return stack_.empty();
        }

        CompResults comp_results() { return comp_results_; }

    private:
        BTree* btree_;
        detail::Stack<std::pair<PageId, uint16_t>, 8> stack_;       // 索引必定是小于等于搜索时key的节点
        CompResults comp_results_{ CompResults::kNe };
    };

public:
    BTree(Pager* pager, PageId& root_pgid) : 
        pager_{ pager }, 
        root_pgid_ { root_pgid }
    {
        auto free_size = pager_->page_size() - (sizeof(Node) - sizeof(Node::body));

        max_leaf_element_count_ = free_size / sizeof(Node::LeafElement);
        max_branch_element_count_ = (free_size - sizeof(PageId)) / sizeof(Node::BranchElement);
    }
    
    /*
    * 叶子节点的分裂
    * 返回新右节点
    */
    Noder Split(Noder* left, uint16_t insert_pos, Span&& insert_key, Span&& insert_value) {
        Noder right{ this, pager_->Alloc(1) };
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
                    left->SpanMove(&right, std::move(insert_key)), 
                    left->SpanMove(&right, std::move(insert_value))
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
                right.LeafSet(right_count, std::move(insert_key), std::move(insert_value));
            }
            else { 
                left->LeafInsert(insert_pos, std::move(insert_key), std::move(insert_value)); 
            }
        }
        right_node->element_count = right_count + insert_right;
        assert(right_node->element_count > 2);

        return right;
    }

    /*
    * 叶子节点的插入
    */
    void Put(Iterator* iter, std::span<const uint8_t> key, std::span<const uint8_t> value) {
        if (iter->Empty()) {
            root_pgid_ = pager_->Alloc(1);
            Noder noder{ this, root_pgid_ };
            noder.LeafBuild();
            noder.LeafInsert(0, noder.SpanAlloc(key), noder.SpanAlloc(value));
            return;
        }

        auto [pgid, pos] = iter->Cur();
        Noder noder{ this, pgid };
        auto key_span = noder.SpanAlloc(key);
        auto value_span = noder.SpanAlloc(value);
        if (iter->comp_results() == Iterator::CompResults::kEq) {
            noder.LeafSet(pos, std::move(key_span), std::move(value_span));
            return;
        }

        if (noder.node()->element_count < max_leaf_element_count_) {
            noder.LeafInsert(pos, std::move(key_span), std::move(value_span));
            return;
        }
        
        // 需要分裂再向上插入
        Noder right = Split(&noder, pos, std::move(key_span), std::move(value_span));

        iter->Pop();
        // 上升右节点的第一个节点
        Put(iter, std::move(noder),
            std::move(right),
            &right.node()->body.leaf[0].key
        );
    }
    

    /*
    * 分支节点的分裂
    * 返回左侧节点中末尾上升的元素，新右节点
    */
    std::tuple<Span, Noder> Split(Noder* left, uint16_t insert_pos, Span&& insert_key, PageId insert_right_child) {
        Noder right{ this, pager_->Alloc(1) };
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
                right.BranchSet(right_node->element_count++, std::move(insert_key), right_node->body.tail_child);
                right_node->body.tail_child = insert_right_child;
            }
            else {
                left->BranchInsert(insert_pos, std::move(insert_key), insert_right_child);
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
    void Put(Iterator* iter, Noder&& left, Noder&& right, Span* key, bool branch_put = false) {
        if (iter->Empty()) {
            root_pgid_ = pager_->Alloc(1);
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
            noder.BranchInsert(0, std::move(noder_key_span), right.page_id());
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
            noder.BranchInsert(pos, std::move(noder_key_span), right.page_id());
            return;
        }

        auto [branch_key, branch_right] = Split(&noder, pos, std::move(noder_key_span), right.page_id());
        iter->Pop();
        Put(iter, std::move(noder), std::move(branch_right), &branch_key, true);
    }


    void Put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
        Iterator iter{ this };
        auto status = iter.Top(key);
        while (status == Iterator::Status::kDown) {
            status = iter.Down(key);
        }
        Put(&iter, key, value);
    }



    bool Get(std::span<const uint8_t> key) {
        Iterator iter{ this };
        auto status = iter.Top(key);
        while (status == Iterator::Status::kDown) {
            status = iter.Down(key);
        }
        return iter.comp_results() == Iterator::CompResults::kEq;
    }


    std::tuple<Noder, uint16_t, Noder, bool> GetSibling(Iterator* iter) {
        auto [parent_pgid, parent_pos] = iter->Cur();
        Noder parent{ this, parent_pgid };
        auto parent_node = parent.node();
        assert(parent_node->element_count > 2);

        bool left_sibling = false;
        PageId sibling_pgid;
        if (parent_pgid == parent_node->body.tail_child) {
            // 是父节点中最大的元素，只能选择左兄弟节点
            left_sibling = true;
            sibling_pgid = parent_node->body.branch[parent_pos - 1].left_child;
        }
        else {
            sibling_pgid = parent.BranchGetLeftChild(parent_pos + 1);
        }

        Noder sibling{ this, sibling_pgid };

        iter->Pop();
        return { std::move(parent), parent_pos, std::move(sibling), left_sibling };
    }


    /*
    * 分支节点的删除
    */
    void Delete(Iterator* iter, Noder&& noder, uint16_t del_pos) {
        auto node = noder.node();

        noder.BranchDelete(del_pos);
        if (node->element_count >= (max_branch_element_count_ >> 1)) {
            return;
        }
        assert(node->element_count > 2);

        if (!iter->Empty()) {
            // 如果没有父节点
            // 是分支节点则判断是否没有任何子节点了，是则变更余下最后一个子节点为根节点，否则直接结束
            return;
        }

        auto [parent, parent_pos, sibling, left_sibling] = GetSibling(iter);
        auto sibling_node = sibling.node();
        if (sibling_node->element_count > (max_leaf_element_count_ >> 1)) {
            // 若兄弟节点内元素数充足
            if (left_sibling) {
                // 左兄弟节点的末尾元素上升到父节点的头部
                // 父节点的对应元素下降到当前节点的头部
                // 上升元素其右子节点挂在下降的父节点元素的左侧
            }
            else {

            }
            return;
        }
    }


    /*
    * 叶子节点的合并
    */
    void Merge(Noder&& left, Noder&& right) {
        auto left_node = left.node();
        auto right_node = right.node();
        for (uint16_t i = 0; i < right_node->element_count; i++) {
            auto key = right.SpanMove(&left, std::move(right_node->body.leaf[i].key));
            auto value = right.SpanMove(&left, std::move(right_node->body.leaf[i].value));

            left.LeafSet(i + left_node->element_count, std::move(key), std::move(value));
        }
        left_node->element_count += right_node->element_count;

        pager_->Free(right.page_id(), 1);
    }


    /*
    * 叶子节点的删除
    */
    void Delete(Iterator* iter, std::span<const uint8_t> key) {
        auto [pgid, pos] = iter->Cur();
        Noder noder{ this, pgid };
        auto node = noder.node();

        noder.LeafDelete(pos);
        if (node->element_count >= (max_leaf_element_count_ >> 1)) {
            return;
        }
        assert(node->element_count > 2);

        iter->Pop();
        if (!iter->Empty()) {
            // 如果没有父节点
            // 是叶子节点就跳过
            return;
        }
        
        auto [parent, parent_pos, sibling, left_sibling] = GetSibling(iter);
        auto parent_node = parent.node();
        auto sibling_node = sibling.node();

        if (sibling_node->element_count > (max_leaf_element_count_ >> 1)) {
            // 若兄弟节点内元素数充足
            Span new_key;
            if (left_sibling) {
                // 左兄弟节点的末尾的元素插入到当前节点的头部
                // 更新父元素key为当前节点的新首元素key
                auto key = sibling.SpanMove(&noder, std::move(sibling_node->body.leaf[--sibling_node->element_count].key));
                auto value = sibling.SpanMove(&noder, std::move(sibling_node->body.leaf[sibling_node->element_count].value));

                new_key = noder.SpanCopy(&parent, key);
                noder.LeafInsert(0, std::move(key), std::move(value));
            }
            else {
                // 右兄弟节点的头部的元素插入到当前节点的尾部
                // 更新父元素key为右兄弟的新首元素
                auto key = sibling.SpanMove(&noder, std::move(sibling_node->body.leaf[0].key));
                auto value = sibling.SpanMove(&noder, std::move(sibling_node->body.leaf[0].value));
                sibling.LeafDelete(0);

                new_key = sibling.SpanCopy(&parent, sibling_node->body.leaf[0].key);
                noder.LeafInsert(node->element_count - 1, std::move(key), std::move(value));
            }
            parent.SpanFree(std::move(parent_node->body.branch[parent_pos].key));
            parent_node->body.branch[parent_pos].key = new_key;
            return;
        }

        // 合并
        if (left_sibling) {
            Merge(std::move(noder), std::move(sibling));
        }
        else {
            Merge(std::move(sibling), std::move(noder));
        }

        // 处理父节点的指向右节点的指针

        // 向上删除父元素
        Delete(iter, std::move(parent), parent_pos);
    }

    bool Delete(std::span<const uint8_t> key) {
        Iterator iter{ this };
        auto status = iter.Top(key);
        while (status == Iterator::Status::kDown) {
            status = iter.Down(key);
        }
        if (iter.comp_results() != Iterator::CompResults::kEq) {
            return false;
        }
        Delete(&iter, key);
        return true;
    }



    void Print(PageId pgid, int level) {
        std::string indent(level * 4, ' ');
        Noder noder{ this, pgid };
        auto node = noder.node();
        if (noder.IsBranch()) {
            Print(node->body.tail_child, level + 1);
            for (int i = node->element_count - 1; i >= 0; i--) {
                std::string key;
                for (int j = 0; j < node->body.branch[i].key.embed.size; j++) {
                    key += std::format("{:02x}", node->body.branch[i].key.embed.data[j]) + " ";
                }
                std::cout << std::format("{}branch::key::{}::level::{}\n", indent, key, level);
                Print(node->body.branch[i].left_child, level + 1);
            }
        }
        else {
            assert(noder.IsLeaf());
            for (int i = node->element_count - 1; i >= 0; i--) {
                std::string key;
                for (int j = 0; j < node->body.leaf[i].key.embed.size; j++) {
                    key += std::format("{:02x}", node->body.leaf[i].key.embed.data[j]) + " ";
                }
                std::cout << std::format("{}leaf::key::{}::level::{}\n", indent, key, level);
            }
        }
    }

    void Print() {
        Print(root_pgid_, 0);
    }


private:
    friend class Noder;

    Pager* pager_;
    // Tx* tx_;

    PageId& root_pgid_; 

    uint16_t max_leaf_element_count_;
    uint16_t max_branch_element_count_;
};

} // namespace yudb