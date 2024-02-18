#pragma once

#include <cassert>

#include <span>
#include <stdexcept>
#include <optional>
#include <variant>
#include <vector>

#include "noncopyable.h"
#include "node_format.h"
#include "page.h"

namespace yudb {

class BTree;

class BranchNode;
class LeafNode;
class Node {
public:
    Node(BTree* btree, PageId page_id, bool dirty);
    Node(BTree* btree, Page page_ref);
    Node(BTree* btree, uint8_t* page_buf);
    ~Node();

    Node(Node&& right) noexcept;
    void operator=(Node&& right) noexcept;

    bool IsLeaf() const;
    bool IsBranch() const;

    Node Copy() const;

    Page Release();

    uint16_t count();
    PageId page_id() const;
    TxId last_modified_txid() const;
    void set_last_modified_txid(TxId txid) { header_->last_modified_txid = txid; }

protected:
    size_t SpaceNeeded(size_t record_length);
    // including the length of the node header.
    size_t SlotSpace(Slot* slots);
    size_t FreeSpace(Slot* slots);
    size_t FreeSpaceAfterCompaction(Slot* slots);
    void Compactify(Slot* slots);
    void CopyRecordRange(Node* dst, Slot* src_slots);
    uint8_t* Ptr();
    uint8_t* GetRecordPtr(Slot* slots, SlotId slot_id);

    PageSize page_size() const;

protected:
    BTree* const btree_;

    std::optional<Page> page_;
    NodeHeader* header_;
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

    std::span<const uint8_t> GetKey(SlotId slot_id);
    std::pair<SlotId, bool> LowerBound(std::span<const uint8_t> key);
    bool Update(SlotId slot_id, std::span<const uint8_t> key, PageId child, bool is_right_child);
    bool Update(SlotId slot_id, std::span<const uint8_t> key);
    bool Append(std::span<const uint8_t> key, PageId child, bool is_right_child);
    bool Insert(SlotId slot_id, std::span<const uint8_t> key, PageId child, bool is_right_child);
    void Delete(SlotId slot_id, bool right_child);
    void Pop(bool right_cbild);

    size_t GetFillRate();

private:
    BranchNodeFormat* node();
    Slot* slots();
    bool RequestSpaceFor(std::span<const uint8_t> key);
    uint8_t* GetKeyPtr(SlotId slot_id);
    void StoreRecord(SlotId slot_id, std::span<const uint8_t> key);
    void DeleteRecord(SlotId slot_id);
    void RestoreRecord(SlotId slot_id, const Slot& saved_slot);
    size_t MaxInlineRecordLength();
};

class LeafNode : public Node {
public:
    using Node::Node;

    void Build();
    void Destroy();

    Slot& GetSlot(SlotId slot_id);
    std::span<const uint8_t> GetKey(SlotId slot_id);
    std::span<const uint8_t> GetValue(SlotId slot_id);
    std::pair<SlotId, bool> LowerBound(std::span<const uint8_t> key);
    bool Update(SlotId slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value);
    bool Append(std::span<const uint8_t> key, std::span<const uint8_t> value);
    bool Insert(SlotId slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value);
    void Delete(SlotId slot_id);
    void Pop();

    size_t GetFillRate();

private:
    BranchNodeFormat* node();
    Slot* slots();
    bool RequestSpaceFor(std::span<const uint8_t> key, std::span<const uint8_t> value);
    uint8_t* GetKeyPtr(SlotId slot_id);
    uint8_t* GetValuePtr(SlotId slot_id);
    void StoreRecord(SlotId slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value);
    void DeleteRecord(SlotId slot_id);
    void RestoreRecord(SlotId slot_id, const Slot& saved_slot);
    size_t MaxInlineRecordLength();
};


} // namespace yudb