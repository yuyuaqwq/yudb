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

namespace yudb {

class BucketImpl;

class BTree : noncopyable {
public:
    using Iterator = BTreeIterator;
    using Comparator = std::strong_ordering(*)(std::span<const uint8_t> key1, std::span<const uint8_t> key2);

public:
    BTree(BucketImpl* bucket, PageId* root_pgid, Comparator comparator);
    ~BTree() = default;

    Iterator LowerBound(std::span<const uint8_t> key);
    Iterator Get(std::span<const uint8_t> key);
    void Insert(std::span<const uint8_t> key, std::span<const uint8_t> value);
    void Put(std::span<const uint8_t> key, std::span<const uint8_t> value);
    void Update(Iterator* iter, std::span<const uint8_t> value);
    bool Delete(std::span<const uint8_t> key);
    void Delete(Iterator* iter);

    void Print(bool str = false);

    Iterator begin() noexcept;
    Iterator end() noexcept;

    auto& bucket() const { return *bucket_; }

private:
    std::tuple<Node, uint16_t, Node, bool> GetSibling(Iterator* iter);

    void Print(bool str, PageId pgid, int level);

    /*
    * 分支节点的合并
    */
    void Merge(NodeImpl&& left, NodeImpl&& right, Cell&& down_key);

    /*
    * 分支节点的删除
    */
    void Delete(Iterator* iter, NodeImpl&& node, uint16_t left_del_pos);

    /*
    * 叶子节点的合并
    */
    void Merge(NodeImpl&& left, NodeImpl&& right);


    /*
    * 分支节点的分裂
    * 返回左侧节点中末尾上升的元素，新右节点
    */
    std::tuple<Cell, NodeImpl> Split(NodeImpl* left, uint16_t insert_pos, Cell&& insert_key, PageId insert_right_child);

    /*
    * 分支节点的插入
    */
    void Put(Iterator* iter, NodeImpl&& left, NodeImpl&& right, Cell* key, bool branch_put = false);
    
    /*
    * 叶子节点的分裂
    * 返回新右节点
    */
    NodeImpl Split(NodeImpl* left, uint16_t insert_pos, std::span<const uint8_t> key, std::span<const uint8_t> value);

    /*
    * 叶子节点的插入
    */
    void Put(Iterator* iter, std::span<const uint8_t> key, std::span<const uint8_t> value, bool insert_only);

private:
    friend class BTreeIterator;

    BucketImpl* const bucket_;
    PageId& root_pgid_;

    Comparator comparator_;
};

} // namespace yudb