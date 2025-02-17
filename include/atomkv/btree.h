// The MIT License (MIT)
// Copyright © 2024 https://github.com/yuyuaqwq
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <span>
#include <algorithm>
#include <iostream>
#include <string>

#include <atomkv/noncopyable.h>
#include <atomkv/page_format.h>
#include <atomkv/comparator.h>
#include <atomkv/btree_iterator.h>
#include <atomkv/node.h>


namespace atomkv {

class BucketImpl;

// B+Tree designed for disk storage
class BTree : noncopyable {
public:
    using Iterator = BTreeIterator;

public:
    BTree(BucketImpl* bucket, PageId* root_pgid, Comparator comparator);
    ~BTree();

    // Check if the tree is empty
    bool Empty() const;

    // Search for the first element less than or equal to the specified key
    // Returns an iterator pointing to the element
    Iterator LowerBound(std::span<const uint8_t> key);

    // Search for the element with the specified key
    // Returns an iterator pointing to the element
    Iterator Get(std::span<const uint8_t> key);

    // Insert a record, allowing duplicate keys
    void Insert(std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket);

    // Insert a record, overwriting the value for duplicate keys
    void Put(std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket);

    // Directly update the value of the element pointed to by the specified iterator
    void Update(Iterator* iter, std::span<const uint8_t> value);

    // Delete the specified element
    bool Delete(std::span<const uint8_t> key);
    void Delete(Iterator* iter);

    // Iterator functions
    Iterator begin() noexcept;
    Iterator end() noexcept;

    auto& bucket() const { return *bucket_; }
    auto& comparator() const { return comparator_.ptr_; }

private:
    // Get the sibling node
    std::tuple<BranchNode, SlotId, PageId, bool> GetSibling(Iterator* iter);

    // Merge branch nodes
    void Merge(BranchNode&& left, BranchNode&& right, std::span<const uint8_t> down_key);

    // Delete from branch nodes
    void Delete(Iterator* iter, BranchNode&& node, SlotId left_del_slot_id);

    // Merge leaf nodes
    void Merge(LeafNode&& left, LeafNode&& right);

    // Split a branch node
    // Returns the last element from the left node to move up and the new right node
    std::tuple<std::span<const uint8_t>, BranchNode> Split(BranchNode* left, SlotId insert_pos, std::span<const uint8_t> key, PageId insert_right_child);

    // Insert into a branch node
    void Put(Iterator* iter, Node&& left, Node&& right, std::span<const uint8_t> key);

    // Split a leaf node
    // Returns the new right node
    LeafNode Split(LeafNode* left, SlotId insert_slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket);

    // Insert into a leaf node
    void Put(Iterator* iter, std::span<const uint8_t> key, std::span<const uint8_t> value, bool insert_only, bool is_bucket);

private:
    friend class BTreeIterator;

    BucketImpl* const bucket_;
    PageId& root_pgid_;

    const Comparator comparator_;
};

} // namespace atomkv
