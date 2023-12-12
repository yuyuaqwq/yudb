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
* |        |
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
                auto [ref, parent_page] = btree_->pager_->Reference(parent_pgid);

                Noder parent{ btree_, parent_page };
                auto parent_node = parent.node();
                if (parent.IsLeaf()) {
                    return Status::kEnd;
                }
                if (pos == parent_node->element_count) {
                    pgid = parent_node->body.tail_child;
                }
                else {
                    pgid = parent_node->body.branch[pos].left_child;
                }
            }

            auto [ref, page] = btree_->pager_->Reference(pgid);
            Noder noder{ btree_, page };
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
    * 返回新右节点的引用
    */
    Pager::PageReference Split(Noder* left, uint16_t insert_pos, Span&& insert_key, Span&& insert_value) {
        auto right_pgid = pager_->Alloc(1);
        auto [right_ref, right_page] = pager_->Reference(right_pgid);
        Noder right{ this, right_page };
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

        return std::move(right_ref);
    }

    /*
    * 叶子节点的插入
    */
    void Put(Iterator* iter, std::span<const uint8_t> key, std::span<const uint8_t> value) {
        if (iter->Empty()) {
            root_pgid_ = pager_->Alloc(1);
            auto [ref, page] = pager_->Reference(root_pgid_);
            Noder noder{ this, page };
            noder.LeafBuild();
            noder.LeafInsert(0, noder.SpanAlloc(key), noder.SpanAlloc(value));
            return;
        }

        auto [pgid, pos] = iter->Cur();
        auto [ref, page] = pager_->Reference(pgid);
        Noder noder{ this, page };
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
        auto right_ref = Split(&noder, pos, std::move(key_span), std::move(value_span));
        Noder right{ this, right_ref.page_cache() };

        iter->Pop();
        // 上升右节点的第一个节点
        Put(iter, std::move(ref), 
            std::move(right_ref),
            &right.node()->body.leaf[0].key
        );
    }
    

    /*
    * 分支节点的分裂
    * 返回左侧节点中末尾上升的元素，新右节点的引用
    */
    std::tuple<Span, Pager::PageReference> Split(Noder* left, uint16_t insert_pos, Span&& insert_key, PageId insert_right_child) {
        auto right_pgid = pager_->Alloc(1);
        auto [right_ref, right_page] = pager_->Reference(right_pgid);
        Noder right{ this, right_page };
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

        return { span, std::move(right_ref) };
    }

    /*
    * 分支节点的插入
    */
    void Put(Iterator* iter, Pager::PageReference&& left_ref, Pager::PageReference&& right_ref, Span* key, bool branch_put = false) {
        Noder left{ this, left_ref.page_cache() };
        Noder right{ this, right_ref.page_cache() };

        if (iter->Empty()) {
            root_pgid_ = pager_->Alloc(1);
            auto [ref, page] = pager_->Reference(root_pgid_);
            Noder noder{ this, page };
            noder.BranchBuild();
            noder.node()->body.tail_child = left_ref.page_id();

            Span noder_key_span;
            if (branch_put) {
                noder_key_span = left.SpanMove(&noder, std::move(*key));
            }
            else {
                noder_key_span = right.SpanCopy(&noder, *key);
            }
            noder.BranchInsert(0, std::move(noder_key_span), right_ref.page_id());
            return;
        }
        
        auto [pgid, pos] = iter->Cur();
        auto [ref, page] = pager_->Reference(pgid);
        Noder noder{ this, page };

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
            noder.BranchInsert(pos, std::move(noder_key_span), right_ref.page_id());
            return;
        }

        auto [branch_key, branch_right_ref] = Split(&noder, pos, std::move(noder_key_span), right_ref.page_id());
        iter->Pop();
        Put(iter, std::move(ref), std::move(branch_right_ref), &branch_key, true);
    }


    void Put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
        Iterator iter{ this };
        auto status = iter.Top(key);
        while (status == Iterator::Status::kDown) {
            status = iter.Down(key);
        }
        Put(&iter, key, value);
    }


    void Print(PageId pgid, int level) {
        std::string indent(level * 4, ' ');
        auto [ref, page] = pager_->Reference(pgid);
        Noder noder{ this, page };
        auto node = noder.node();
        if (noder.IsBranch()) {
            Print(node->body.tail_child, level + 1);
            for (int i = node->element_count - 1; i >= 0; i--) {
                std::string key;
                for (int j = 0; j < node->body.branch[i].key.embed.size; j++) {
                    key += std::format("{:02x}", node->body.branch[i].key.embed.data[j]) + " ";
                }
                std::cout << std::format("{}branch::key::{}::level::{}\n", indent, key, level);
                Print(node->body.branch[i].left_child, level+1);
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


    bool Find(std::span<const uint8_t> key) {
        Iterator iter{ this };
        auto status = iter.Top(key);
        while (status == Iterator::Status::kDown) {
            status = iter.Down(key);
        }
        return iter.comp_results() == Iterator::CompResults::kEq;
    }

private:
    friend class Noder;

    Pager* pager_;
    // Tx* tx_;

    PageId& root_pgid_; // PageId&

    uint16_t max_leaf_element_count_;
    uint16_t max_branch_element_count_;
};

} // namespace yudb