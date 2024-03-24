//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <cassert>

#include <span>
#include <stdexcept>
#include <optional>
#include <variant>
#include <vector>

#include "yudb/node_format.h"
#include "yudb/page.h"
#include "yudb/noncopyable.h"

namespace yudb {

class BTree;

class Node {
public:
    Node(BTree* btree, PageId page_id, bool dirty);
    Node(BTree* btree, Page page_ref);
    Node(BTree* btree, uint8_t* page_buf);
    virtual ~Node();

    Node(Node&& right) noexcept;
    void operator=(Node&& right) noexcept;

    bool IsLeaf() const;
    bool IsBranch() const;

    std::span<const uint8_t> GetKey(SlotId slot_id);
    std::pair<SlotId, bool> LowerBound(std::span<const uint8_t> key);

    double GetFillRate();

    Node Copy() const;
    Node AddReference() const;
    Page Release();

    const auto& header() const { return struct_->header; }
    uint16_t count() const;
    Slot* slots() const { return struct_->slots; }
    PageId page_id() const;
    TxId last_modified_txid() const;
    void set_last_modified_txid(TxId txid) { struct_->header.last_modified_txid = txid; }

protected:
    PageSize MaxInlineRecordSize();

    bool RequestSpaceFor(std::span<const uint8_t> key, std::span<const uint8_t> value, bool slot_needed);
    size_t SpaceNeeded(size_t record_size, bool slot_needed);
    // 包括节点头的大小
    PageSize SlotSpace();
    PageSize FreeSpace();
    PageSize FreeSpaceAfterCompaction();
    void Compactify();

    uint8_t* Ptr();
    uint8_t* GetRawRecordPtr(SlotId slot_id);
    uint8_t* GetRecordPtr(SlotId slot_id);

    PageId StoreRecordToOverflowPages(SlotId slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value);
    void StoreRecord(SlotId slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value);
    void CopyRecordRange(Node* dst);
    void DeleteRecord(SlotId slot_id);
    void RestoreRecord(SlotId slot_id, const Slot& saved_slot);

    PageSize page_size() const;

protected:
    BTree* const btree_;

    std::optional<Page> page_;
    NodeStruct* struct_;
};

class BranchNode : public Node {
public:
    using Node::Node;

    void Build(PageId tail_child);
    void Destroy();

    PageId GetLeftChild(SlotId slot_id);
    void SetLeftChild(SlotId slot_id, PageId child);
    PageId GetRightChild(SlotId slot_id);
    void SetRightChild(uint16_t slot_id, PageId child);
    PageId GetTailChild();
    void SetTailChild(PageId child);

    bool Update(SlotId slot_id, std::span<const uint8_t> key, PageId child, bool is_right_child);
    bool Update(SlotId slot_id, std::span<const uint8_t> key);
    bool Append(std::span<const uint8_t> key, PageId child, bool is_right_child);
    bool Insert(SlotId slot_id, std::span<const uint8_t> key, PageId child, bool is_right_child);
    void Delete(SlotId slot_id, bool right_child);
    void Pop(bool right_cbild);
};

class LeafNode : public Node {
public:
    using Node::Node;

    void Build();
    void Destroy();

    Slot& GetSlot(SlotId slot_id);
    bool IsBucket(SlotId slot_id) const;
    void SetIsBucket(SlotId slot_id, bool b);
    std::span<const uint8_t> GetValue(SlotId slot_id);
    bool Update(SlotId slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value);
    bool Append(std::span<const uint8_t> key, std::span<const uint8_t> value);
    bool Insert(SlotId slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value);
    void Delete(SlotId slot_id);
    void Pop();
};

} // namespace yudb