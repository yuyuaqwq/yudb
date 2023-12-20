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

class Tx;
class Bucket;

/*
* b+tree
*/
class BTree {
public:
    class Iterator {
    public:
        using iterator_category = std::bidirectional_iterator_tag;

        using value_type = typename Iterator;
        using difference_type = typename std::ptrdiff_t;
        using pointer = typename Iterator*;
        using reference = const value_type&;

    private:
        typedef enum class Status {
            kDown,
            kNext,
            kEnd,
        };

        typedef enum class CompResult {
            kInvalid,
            kEq,
            kGt,
            kLt,
        };

    public:
        reference operator*() const noexcept {
            return *this;
        }

        //pointer operator->() noexcept {
        //    return this;
        //}

        const Iterator* operator->() const noexcept {
            return this;
        }

        Iterator& operator++() noexcept {
            Next();
            return *this;
        }

        Iterator operator++(int) noexcept {
            Iterator tmp = *this;
            tmp.Next();
            return tmp;
        }

        Iterator& operator--() noexcept {
            Prev();
            return *this;
        }

        Iterator operator--(int) noexcept {
            Iterator tmp = *this;
            tmp.Prev();
            return tmp;
        }

        bool operator==(const Iterator& right) const noexcept {
            if (Empty() || right.Empty()) {
                return Empty() && right.Empty();
            }
            auto& [pgid, index] = Cur();
            auto& [pgid2, index2] = right.Cur();
            return btree_ == right.btree_ && pgid == pgid2 && index == index2;
        }

        
        template <class KeyT>
        KeyT key() const {
            auto [buf, size, ref] = KeySpan();
            if (size != sizeof(KeyT)) {
                std::runtime_error("The size of the key does not match.");
            }
            KeyT key;
            memcpy(&key, buf, size);
            return key;
        }

        template <class ValueT>
        ValueT value() const {
            auto [buf, size, ref] = ValueSpan();
            if (size != sizeof(ValueT)) {
                std::runtime_error("The size of the value does not match.");
            }
            ValueT value;
            memcpy(&value, buf, size);
            return value;
        }


        std::string key() const {
            auto [buf, size, ref] = KeySpan();
            std::string val{reinterpret_cast<const char*>(buf), size};
            return val;
        }

        std::string value() const {
            auto [buf, size, ref] = ValueSpan();
            std::string val{ reinterpret_cast<const char*>(buf), size };
            return val;
        }


    private:
        void First(PageId pgid) {
            do {
                Noder noder{ btree_, pgid };
                auto node = noder.node();
                if (noder.IsLeaf()) {
                    break;
                }
                assert(node->element_count > 0);
                pgid = node->body.branch[0].left_child;
                stack_.push_back({ pgid, 0 });
            } while (true);
            Noder noder{ btree_, pgid };
            auto node = noder.node();
            stack_.push_back({ pgid, 0 });
        }

        void Last(PageId pgid) {
            do {
                Noder noder{ btree_, pgid };
                auto node = noder.node();
                if (noder.IsLeaf()) {
                    break;
                }
                assert(node->element_count > 0);
                pgid = node->body.branch[node->element_count - 1].left_child;
                stack_.push_back({ pgid, node->element_count - 1 });
            } while (true);
            Noder noder{ btree_, pgid };
            auto node = noder.node();
            stack_.push_back({ pgid, node->element_count - 1 });
        }

        void Next() {
            do {
                auto& [pgid, index] = Cur();
                Noder noder{ btree_, pgid };
                auto node = noder.node();

                if (++index < node->element_count) {
                    if (noder.IsLeaf()) {
                        return;
                    }
                    First(node->body.branch[index].left_child);
                    return;
                }
                if (noder.IsBranch() && index == node->element_count) {
                    First(node->body.tail_child);
                    return;
                }
                Pop();
            } while (!Empty());
        }

        void Prev() {
            do {
                auto& [pgid, index] = Cur();
                Noder noder{ btree_, pgid };
                auto node = noder.node();

                if (index > 0) {
                    --index;
                    if (noder.IsLeaf()) {
                        return;
                    }
                    Last(node->body.branch[index].left_child);
                    return;
                }
                Pop();
            } while (!Empty());
        }


        std::tuple<const uint8_t*, size_t, std::optional<std::variant<PageReferencer, std::vector<uint8_t>>>>
        KeySpan() const {
            auto& [pgid, index] = Cur();
            Noder noder{ btree_, pgid };
            if (!noder.IsLeaf()) {
                throw std::runtime_error("not pointing to valid data");
            }
            auto res = noder.SpanLoad(noder.node()->body.leaf[index].key);
            return res;
        }

        std::tuple<const uint8_t*, size_t, std::optional<std::variant<PageReferencer, std::vector<uint8_t>>>>
        ValueSpan() const {
            auto& [pgid, index] = Cur();
            Noder noder{ btree_, pgid };
            if (!noder.IsLeaf()) {
                throw std::runtime_error("not pointing to valid data");
            }
            auto res = noder.SpanLoad(noder.node()->body.leaf[index].value);
            return res;
        }

    private:
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
                auto iter = std::lower_bound(noder.begin(), noder.end(), key, [&](const Span& span, std::span<const uint8_t> search_key) -> bool {
                    auto [buf, size, ref] = noder.SpanLoad(span);
                    auto res = memcmp(buf, search_key.data(), std::min(size, search_key.size()));
                    if (res == 0 && size != search_key.size()) {
                        comp_result_ = size < search_key.size() ? CompResult::kLt : CompResult::kGt;
                        return comp_result_ == CompResult::kLt;
                    }
                    if (res != 0) {
                        comp_result_ = res < 0 ? CompResult::kLt : CompResult::kGt;
                        return comp_result_ == CompResult::kLt;
                    }
                    else {
                        comp_result_ = CompResult::kEq;
                        return false;
                    }
                    });
                index = iter.index();
                if (comp_result_ == CompResult::kEq && noder.IsBranch()) {
                    ++index;
                }
            }
            else {
                index = 0;
            }
            stack_.push_back(std::pair{ pgid, index });
            return Status::kDown;
        }


        std::pair<PageId, uint16_t>& Cur() {
            return stack_.front();
        }

        const std::pair<PageId, uint16_t>& Cur() const {
            return stack_.front();
        }


        void Pop() {
            stack_.pop_back();
        }

        bool Empty() const {
            return stack_.empty();
        }


        std::pair<PageId, uint16_t>& Index(ptrdiff_t i) { return stack_.index(i); }

        size_t Size() { return stack_.cur_pos(); }

        CompResult comp_result() { return comp_result_; }

    private:
        friend class BTree;

        BTree* btree_;
        detail::Stack<std::pair<PageId, uint16_t>, 8> stack_;       // 索引必定是小于等于搜索时key的节点
        CompResult comp_result_{ CompResult::kInvalid };
    };

public:
    BTree(Pager* pager, Tx* tx, PageId& root_pgid) : 
        pager_{ pager }, 
        tx_{ tx },
        root_pgid_ { root_pgid }
    {
        auto free_size = pager_->page_size() - (sizeof(Node) - sizeof(Node::body));

        max_leaf_element_count_ = free_size / sizeof(Node::LeafElement);
        max_branch_element_count_ = (free_size - sizeof(PageId)) / sizeof(Node::BranchElement);
    }

    void Put(std::span<const uint8_t> key, std::span<const uint8_t> value) {
        Iterator iter{ this };
        auto status = iter.Top(key);
        while (status == Iterator::Status::kDown) {
            status = iter.Down(key);
        }
        Put(&iter, key, value);
    }

    Iterator Get(std::span<const uint8_t> key) {
        Iterator iter{ this };
        auto status = iter.Top(key);
        while (status == Iterator::Status::kDown) {
            status = iter.Down(key);
        }
        if (iter.comp_result() != Iterator::CompResult::kEq) {
            return Iterator{ this };
        }
        return iter;
    }

    bool Delete(std::span<const uint8_t> key) {
        Iterator iter{ this };
        auto status = iter.Top(key);
        while (status == Iterator::Status::kDown) {
            status = iter.Down(key);
        }
        if (iter.comp_result() != Iterator::CompResult::kEq) {
            return false;
        }
        Delete(&iter, key);
        return true;
    }


    Iterator begin() noexcept {
        Iterator iter { this };
        iter.First(root_pgid_);
        return iter;
    }

    Iterator end() noexcept {
        return Iterator{ this };
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
    std::tuple<Noder, uint16_t, Noder, bool> GetSibling(Iterator* iter) {
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

    void PathPageCopy(Iterator* iter);


    /*
    * 分支节点的合并
    */
    void Merge(Noder&& left, Noder&& right, Span&& down_key) {
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
        pager_->Free(right.page_id(), 1);
    }

    /*
    * 分支节点的删除
    */
    void Delete(Iterator* iter, Noder&& noder, uint16_t left_del_pos) {
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
                pager_->Free(old_root, 1);
            }
            return;
        }

        assert(node->element_count > 0);

        auto [parent, parent_pos, sibling, left_sibling] = GetSibling(iter);
        if (left_sibling) --parent_pos;
        auto sibling_node = sibling.node();
        auto parent_node = parent.node();
        if (sibling_node->element_count > (max_leaf_element_count_ >> 1)) {
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
                auto parent_key = parent.SpanMove(&noder, std::move(parent_node->body.branch[parent_pos].key));
                auto sibling_key = sibling.SpanMove(&parent, std::move(sibling_node->body.branch[sibling_node->element_count - 1].key));

                noder.BranchInsert(0, std::move(parent_key), sibling_node->body.tail_child, false);
                sibling.BranchDelete(sibling_node->element_count - 1, true);

                parent_node->body.branch[parent_pos].key = std::move(sibling_key);
            }
            else {
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
        auto sibling_node = sibling.node();

        if (sibling_node->element_count > (max_leaf_element_count_ >> 1)) {
            // 若兄弟节点内元素数充足
            Span new_key;
            if (left_sibling) {
                // 左兄弟节点的末尾的元素插入到当前节点的头部
                // 更新父元素key为当前节点的新首元素key
                auto key = sibling.SpanMove(&noder, std::move(sibling_node->body.leaf[sibling_node->element_count - 1].key));
                auto value = sibling.SpanMove(&noder, std::move(sibling_node->body.leaf[sibling_node->element_count - 1].value));
                sibling.LeafDelete(sibling_node->element_count - 1);

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
        if (iter->comp_result() == Iterator::CompResult::kEq) {
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


private:
    friend class Noder;
    friend class Bucket;

    Pager* pager_;
    Tx* tx_;

    PageId& root_pgid_; 

    uint16_t max_leaf_element_count_;
    uint16_t max_branch_element_count_;
};

} // namespace yudb