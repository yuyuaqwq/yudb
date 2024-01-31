#pragma once

#include <span>
#include <algorithm>
#include <iostream>
#include <format>
#include <string>

#include "noncopyable.h"
#include "btree_iterator.h"
#include "page_format.h"
#include "node.h"

#undef min

namespace yudb {

class BucketImpl;

class BTree : noncopyable {
public:
    using Iterator = BTreeIterator;
    using Comparator = std::strong_ordering(*)(std::span<const uint8_t> key1, std::span<const uint8_t> key2);

public:
    BTree(BucketImpl* bucket, PageId* root_pgid, Comparator comparator);

    ~BTree() = default;

    BTree(BTree&& right) noexcept {
        operator=(std::move(right));
    }
    void operator=(BTree&& right) noexcept {
        bucket_ = nullptr;
        root_pgid_ = nullptr;
        comparator_ = std::move(right.comparator_);
        right.root_pgid_ = nullptr;
    }

    void set_bucket(BucketImpl* bucket) { bucket_ = bucket; }
    void set_root_pgid(PageId* root_pgid) { root_pgid_ = root_pgid; }
    BucketImpl& bucket() const { return *bucket_; }

    Iterator LowerBound(std::span<const uint8_t> key);
    Iterator Get(std::span<const uint8_t> key);
    void Insert(std::span<const uint8_t> key, std::span<const uint8_t> value);
    void Put(std::span<const uint8_t> key, std::span<const uint8_t> value);
    void Update(Iterator* iter, std::span<const uint8_t> value);
    bool Delete(std::span<const uint8_t> key);
    void Delete(Iterator* iter);

    void Print(bool str = false) const;

    Iterator begin() const noexcept;
    Iterator end() const noexcept;

private:
    std::tuple<MutNode, uint16_t, MutNode, bool> GetSibling(Iterator* iter);

    void Print(bool str, PageId pgid, int level) const;

    /*
    * 分支节点的合并
    */
    void Merge(Node&& left, Node&& right, Cell&& down_key);

    /*
    * 分支节点的删除
    */
    void Delete(Iterator* iter, Node&& node, uint16_t left_del_pos);

    /*
    * 叶子节点的合并
    */
    void Merge(Node&& left, Node&& right);


    /*
    * 分支节点的分裂
    * 返回左侧节点中末尾上升的元素，新右节点
    */
    std::tuple<Cell, Node> Split(Node* left, uint16_t insert_pos, Cell&& insert_key, PageId insert_right_child);

    /*
    * 分支节点的插入
    */
    void Put(Iterator* iter, Node&& left, Node&& right, Cell* key, bool branch_put = false);
    
    /*
    * 叶子节点的分裂
    * 返回新右节点
    */
    Node Split(Node* left, uint16_t insert_pos, std::span<const uint8_t> key, std::span<const uint8_t> value);

    /*
    * 叶子节点的插入
    */
    void Put(Iterator* iter, std::span<const uint8_t> key, std::span<const uint8_t> value, bool insert_only);

private:
    friend class BTreeIterator;

    BucketImpl* bucket_;
    PageId* root_pgid_; 

    Comparator comparator_;
};

} // namespace yudb