#pragma once

#include <span>
#include <algorithm>

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
                if (parent.IsLeaf()) {
                    return Status::kEnd;
                }
                pgid = parent.node()->branch[pos].little_child;
            }

            auto [ref, page] = btree_->pager_->Reference(pgid);
            Noder noder{ btree_, page };

            // 在节点中进行二分查找
            uint16_t index;
            if (noder.node()->element_count > 0) {
                auto iter = std::lower_bound(noder.begin(), noder.end(), key, [&](const Span& span, std::span<const uint8_t> search_key) -> bool {
                    auto [buf, size] = noder.SpanLoad(span);
                    auto res = memcmp(buf, search_key.data(), std::min(size, search_key.size()));
                    if (res == 0 && size != search_key.size()) {
                        comp_results_ = CompResults::kNe;
                        return size < search_key.size();
                    }
                    if (res < 0) {
                        comp_results_ = CompResults::kNe;
                        return true;
                    }
                    else {
                        comp_results_ = CompResults::kEq;
                        return false;
                    }
                });
                index = iter.index();
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
        max_leaf_element_count_ = (pager_->page_count() - sizeof(Node)) / sizeof(Node::LeafElement);
        max_branch_element_count_ = (pager_->page_count() - sizeof(Node)) / sizeof(Node::BranchElement);
    }
    
    void Split(Noder* left, uint16_t pos) {
        auto right_pgid = pager_->Alloc(1);
        auto [ref, right_page] = pager_->Reference(right_pgid);
        Noder right{ this, right_page };

        uint16_t mid;
        if (left->IsBranch()) {
            mid = max_branch_element_count_ / 2;
        }
        else {
            assert(left->IsLeaf());
            mid = max_leaf_element_count_ / 2;
        }
        
        if (pos > mid) {
            // 新节点插入到右侧


        }
        else {
            // 新节点插入到左侧

        }

    }


    void Put(Iterator* iter, std::span<const uint8_t> key, std::span<const uint8_t> value) {
        if (iter->Empty()) {
            root_pgid_ = pager_->Alloc(1);
            auto [ref, page] = pager_->Reference(root_pgid_);
            Noder noder{ this, page };
            noder.LeafBuild();
            noder.LeafInsert(0, noder.SpanSave(key), noder.SpanSave(value));
            return;
        }

        auto [pgid, pos] = iter->Cur();
        auto [ref, page] = pager_->Reference(root_pgid_);
        Noder noder{ this, page };
        auto key_spaner = noder.SpanSave(key);
        auto value_spaner = noder.SpanSave(value);
        if (iter->comp_results() == Iterator::CompResults::kEq) {
            noder.LeafSet(pos, std::move(key_spaner), std::move(value_spaner));
            return;
        }

        if (noder.node()->element_count < max_leaf_element_count_) {
            noder.LeafInsert(pos, std::move(key_spaner), std::move(value_spaner));
            return;
        }
        
        // 需要分裂再向上插入

        

    }
    
    void Put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
        Iterator iter{ this };
        auto status = iter.Top(key);
        while (status == Iterator::Status::kDown) {
            status = iter.Down(key);
        }
        Put(&iter, key, value);
    }


private:
    void SpanMove(Span& dst, Span& src) {

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