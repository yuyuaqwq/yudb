//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <span>
#include <algorithm>
#include <iostream>
#include <string>

#include "yudb/btree_iterator.h"
#include "yudb/comparator.h"
#include "yudb/node.h"
#include "yudb/noncopyable.h"
#include "yudb/page_format.h"

namespace yudb {

class BucketImpl;


// 为磁盘设计的B+Tree
class BTree : noncopyable {
public:
    using Iterator = BTreeIterator;

public:
    BTree(BucketImpl* bucket, PageId* root_pgid, Comparator comparator);
    ~BTree();

    // 树是否为空
    bool Empty() const;

    // 搜索第一个小于等于指定key的元素
    // 返回指向元素的迭代器
    Iterator LowerBound(std::span<const uint8_t> key);

    // 搜索指定key的元素
    // 返回指向元素的迭代器
    Iterator Get(std::span<const uint8_t> key);

    // 插入记录，允许重复key
    void Insert(std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket);

    // 插入记录，覆盖重复key对应的value
    void Put(std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket);

    // 直接更新指定迭代器指向的元素的value
    void Update(Iterator* iter, std::span<const uint8_t> value);

    // 删除指定元素
    bool Delete(std::span<const uint8_t> key);
    void Delete(Iterator* iter);

    //void Print(bool str = false);

    Iterator begin() noexcept;
    Iterator end() noexcept;

    auto& bucket() const { return *bucket_; }
    auto& comparator() const { return comparator_.ptr_;  }

private:
    // 获取兄弟节点
    std::tuple<BranchNode, SlotId, PageId, bool> GetSibling(Iterator* iter);

    //void Print(bool str, PageId pgid, int level);

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
    void Put(Iterator* iter, Node&& left, Node&& right, std::span<const uint8_t> key);
    
    // 叶子节点的分裂
    // 返回新右节点
    LeafNode Split(LeafNode* left, SlotId insert_slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket);

    // 叶子节点的插入
    void Put(Iterator* iter, std::span<const uint8_t> key, std::span<const uint8_t> value, bool insert_only, bool is_bucket);

private:
    friend class BTreeIterator;

    BucketImpl* const bucket_;
    PageId& root_pgid_;

    const Comparator comparator_;
};

} // namespace yudb