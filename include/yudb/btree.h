#pragma once

#include <span>
#include <algorithm>
#include <iostream>
#include <format>
#include <string>

#include "yudb/btree_iterator.h"
#include "yudb/comparator.h"
#include "yudb/node.h"
#include "yudb/noncopyable.h"
#include "yudb/page_format.h"

namespace yudb {

class BucketImpl;

class BTree : noncopyable {
public:
    using Iterator = BTreeIterator;

public:
    BTree(BucketImpl* bucket, PageId* root_pgid, const Comparator comparator);
    ~BTree();

    bool Empty() const;
    Iterator LowerBound(std::span<const uint8_t> key);
    Iterator Get(std::span<const uint8_t> key);
    void Insert(std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket);
    void Put(std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket);
    void Update(Iterator* iter, std::span<const uint8_t> value);
    bool Delete(std::span<const uint8_t> key);
    void Delete(Iterator* iter);

    void Print(bool str = false);

    Iterator begin() noexcept;
    Iterator end() noexcept;

    auto& bucket() const { return *bucket_; }
    auto& comparator() const { return comparator_;  }

private:
    std::tuple<BranchNode, SlotId, PageId, bool> GetSibling(Iterator* iter);

    void Print(bool str, PageId pgid, int level);

    // 分支节点的合并
    void Merge(BranchNode&& left, BranchNode&& right, std::span<const uint8_t> down_key);

    // 分支节点的删除
    void Delete(Iterator* iter, BranchNode&& node, SlotId left_del_slot_id);

    // 叶子节点的合并
    void Merge(LeafNode&& left, LeafNode&& right);


    // 分支节点的分裂
    // 返回左侧节点中末尾上升的元素，新右节点
    std::tuple<std::span<const uint8_t>, BranchNode> Split(BranchNode* left, SlotId insert_pos, std::span<const uint8_t> key, PageId insert_right_child);

    // 分支节点的插入
    void Put(Iterator* iter, Node&& left, Node&& right, std::span<const uint8_t> key, bool branch_put = false);
    
    // 叶子节点的分裂
    // 返回新右节点
    LeafNode Split(LeafNode* left, SlotId insert_slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket);

    // 叶子节点的插入
    void Put(Iterator* iter, std::span<const uint8_t> key, std::span<const uint8_t> value, bool insert_only, bool is_bucket);

private:
    friend class BTreeIterator;

    BucketImpl* const bucket_;
    PageId& root_pgid_;

    Comparator comparator_;
};

} // namespace yudb