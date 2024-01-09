#pragma once

#include <span>
#include <algorithm>
#include <iostream>
#include <format>
#include <string>

#include "btree_iterator.h"
#include "page.h"
#include "noder.h"

#undef min

namespace yudb {

class Bucket;

class BTree {
public:
    using Iterator = BTreeIterator;

public:
    BTree(Bucket* bucket, PageId* root_pgid);

    ~BTree() = default;

    Iterator LowerBound(std::span<const uint8_t> key) const;

    BTreeIterator Get(std::span<const uint8_t> key) const;

    void Put(std::span<const uint8_t> key, std::span<const uint8_t> value);

    bool Delete(std::span<const uint8_t> key);

    void Print(bool str = false) const;


    Iterator begin() const noexcept;

    Iterator end() const noexcept;


    Bucket& bucket() const { return *bucket_; }

private:
    std::tuple<Noder, uint16_t, Noder, bool> GetSibling(Iterator* iter);

    void Print(bool str, PageId pgid, int level) const;

    /*
    * 分支节点的合并
    */
    void Merge(Noder&& left, Noder&& right, Span&& down_key);

    /*
    * 分支节点的删除
    */
    void Delete(Iterator* iter, Noder&& noder, uint16_t left_del_pos);

    /*
    * 叶子节点的合并
    */
    void Merge(Noder&& left, Noder&& right);

    /*
    * 叶子节点的删除
    */
    void Delete(Iterator* iter, std::span<const uint8_t> key);


    /*
    * 分支节点的分裂
    * 返回左侧节点中末尾上升的元素，新右节点
    */
    std::tuple<Span, Noder> Split(Noder* left, uint16_t insert_pos, Span&& insert_key, PageId insert_right_child_pgid);

    /*
    * 分支节点的插入
    */
    void Put(Iterator* iter, Noder&& left, Noder&& right, Span* key, bool from_branch = false);
    
    /*
    * 叶子节点的分裂
    * 返回新右节点
    */
    Noder Split(Noder* left, uint16_t insert_pos, std::span<const uint8_t> key, std::span<const uint8_t> value);

    /*
    * 叶子节点的插入
    */
    void Put(Iterator* iter, std::span<const uint8_t> key, std::span<const uint8_t> value);


    void PathCopy(Iterator* iter);

private:
    friend class BTreeIterator;

    Bucket* bucket_;
    PageId* root_pgid_; 
};

} // namespace yudb