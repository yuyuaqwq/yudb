#include "yudb/node.h"

#include "yudb/bucket_impl.h"
#include "yudb/pager.h"
#include "yudb/tx.h"
#include "yudb/error.h"

namespace yudb {

constexpr PageCount kMaxCachedPageCount = 32;

Node::Node(BTree* btree, PageId page_id, bool dirty) :
    btree_{ btree },
    page_{ btree_->bucket().pager().Reference(page_id, dirty) },
    struct_{ reinterpret_cast<NodeStruct*>(page_->page_buf()) } {}

Node::Node(BTree* btree, Page page_ref) :
    btree_{ btree },
    page_{ std::move(page_ref) },
    struct_{ reinterpret_cast<NodeStruct*>(page_->page_buf()) } {}

Node::Node(BTree* btree, uint8_t* page_buf) :
    btree_{ btree },
    struct_{ reinterpret_cast<NodeStruct*>(page_buf) } {}

Node::~Node() = default;

Node::Node(Node&& right) noexcept : 
    btree_{ right.btree_ },
    page_{ std::move(right.page_) },
    struct_{ reinterpret_cast<NodeStruct*>(page_->page_buf()) } {}

void Node::operator=(Node&& right) noexcept {
    assert(btree_ == right.btree_);
    page_ = std::move(right.page_);
    struct_ = reinterpret_cast<NodeStruct*>(page_->page_buf());
}

bool Node::IsLeaf() const {
    return struct_->header.type == NodeType::kLeaf;
}

bool Node::IsBranch() const {
    return struct_->header.type == NodeType::kBranch;
}

std::span<const uint8_t> Node::GetKey(SlotId slot_id) {
    assert(slot_id < count());
    auto& slot = struct_->slots[slot_id];
    if (!slot.is_overflow_pages) {
        return { GetRecordPtr(slot_id), slot.key_length };
    }
    if (cached_slot_id_ != slot_id || cached_key_page_.has_value()) {
        auto overflow_record = reinterpret_cast<OverflowRecord*>(GetRecordPtr(slot_id));
        auto& pager = btree_->bucket().pager();
        cached_key_page_ = pager.Reference(overflow_record->pgid, false);
        cached_slot_id_ = slot_id;
    }
    assert(cached_key_page_.has_value());
    return { reinterpret_cast<const uint8_t*>(cached_key_page_->page_buf()), slot.key_length };
}

std::pair<SlotId, bool> Node::LowerBound(std::span<const uint8_t> key) {
    bool eq = false;
    auto res = std::lower_bound(struct_->slots, struct_->slots + count(), key, [&](const Slot& slot, std::span<const uint8_t> search_key) -> bool {
        SlotId slot_id = &slot - struct_->slots;
        auto res = btree_->comparator()(GetKey(slot_id), search_key);
        if (res == 0) eq = true;
        return res < 0;
    });
    return { res - struct_->slots, eq };
}

double Node::GetFillRate() {
    auto max_space = page_size() - sizeof(struct_->header) - sizeof(struct_->padding);
    auto used_space = max_space - FreeSpaceAfterCompaction();
    return double(used_space) / max_space;
}

Node Node::Copy() const {
    auto& pager = btree_->bucket().pager();
    auto& tx = btree_->bucket().tx();
    assert(page_.has_value());
    Node node{ btree_, pager.Copy(*page_) };
    node.set_last_modified_txid(tx.txid());
    return node;
}

Node Node::AddReference() const {
    assert(page_.has_value());
    return Node{ btree_, page_->AddReference() };
}

Page Node::Release() {
    assert(page_.has_value());
    return std::move(*page_);
}

PageId Node::page_id() const {
    return page_->page_id();
}

TxId Node::last_modified_txid() const {
    return struct_->header.last_modified_txid;
}
 
uint16_t Node::count() const {
    return struct_->header.count;
}

PageSize Node::page_size() const {
    return btree_->bucket().pager().page_size();
}

size_t Node::MaxInlineRecordLength() {
    size_t max_length;
    max_length = page_size() -
        sizeof(NodeStruct::header) -
        sizeof(NodeStruct::padding);
    // ensure that each node can hold two records.
    assert(max_length % 2 == 0);
    return max_length / 2;
}

size_t Node::SpaceNeeded(size_t record_length) {
    return record_length + sizeof(Slot);
}

bool Node::RequestSpaceFor(std::span<const uint8_t> key, std::span<const uint8_t> value) {
    auto length = key.size() + value.size();
    auto space_needed = SpaceNeeded(length);
    if (space_needed > MaxInlineRecordLength()) {
        space_needed = SpaceNeeded(sizeof(OverflowRecord));
    }
    if (space_needed <= FreeSpace()) {
        return true;
    }
    if (space_needed <= FreeSpaceAfterCompaction()) {
        Compactify();
        return true;
    }
    return false;
}

size_t Node::SlotSpace() {
    auto slot_space = reinterpret_cast<const uint8_t*>(struct_->slots + struct_->header.count) - Ptr();
    assert(slot_space < page_size());
    return slot_space;
}

size_t Node::FreeSpace() {
    auto free_space = struct_->header.data_offset - SlotSpace();
    assert(free_space < page_size());
    return free_space;
}

size_t Node::FreeSpaceAfterCompaction() {
    auto free_space = page_size() - SlotSpace() - struct_->header.space_used;
    assert(free_space < page_size());
    return free_space;
}

void Node::Compactify() {
    auto& pager = btree_->bucket().pager();
    auto& tmp_page = pager.tmp_page();
    Node tmp_node{ btree_, tmp_page };
    std::memset(&tmp_node.struct_->header, 0, sizeof(tmp_node.struct_->header));

    tmp_node.struct_->header.data_offset = pager.page_size();
    CopyRecordRange(&tmp_node);
    assert(struct_->header.space_used == tmp_node.struct_->header.space_used);
    struct_->header.data_offset = tmp_node.struct_->header.data_offset;
    std::memcpy(Ptr() + struct_->header.data_offset, 
        tmp_node.Ptr() + tmp_node.struct_->header.data_offset,
        tmp_node.struct_->header.space_used);
}

uint8_t* Node::Ptr() {
    return reinterpret_cast<uint8_t*>(struct_);
}

uint8_t* Node::GetRecordPtr(SlotId slot_id) {
    assert(slot_id < count()); 
    return Ptr() + struct_->slots[slot_id].record_offset;
}

PageId Node::StoreRecordToOverflowPages(SlotId slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    auto& pager = btree_->bucket().pager();
    auto page_size_ = page_size();

    auto length = key.size() + value.size();
    auto page_count = length / page_size_;
    if (length % page_size_) ++page_count;
    auto pgid = pager.Alloc(page_count);
    if (page_count <= kMaxCachedPageCount) {
        uint32_t remaining_length = key.size();
        auto* data = &key;
        auto offset = 0;
        for (uint32_t i = 0; i < page_count; i++) {
            if (remaining_length == 0) {
                assert(data == &key);
                data = &value;
                remaining_length = value.size();
                offset = page_size_ - key.size() % page_size_;
            }
            auto page_ref = pager.Reference(pgid + i, true);
            auto page_buf = page_ref.page_buf();
            auto copy_length = std::min(static_cast<uint32_t>(page_size_), remaining_length);
            std::memcpy(page_buf, &data->data()[i * page_size_ - offset], copy_length);
            remaining_length -= copy_length;
        }
    } else {
        pager.WriteByBytes(pgid, 0, key.data(), key.size());
        if (!value.empty()) {
            auto key_page_count = key.size() / page_size_;
            pager.WriteByBytes(pgid + key_page_count, key.size() % page_size_, value.data(), value.size());
        }
    }
    return pgid;
}

void Node::LoadRecordFromOverflowPages(SlotId slot_id) {
    auto& slot = struct_->slots[slot_id];
    assert(slot.is_overflow_pages);
    
    auto& pager = btree_->bucket().pager();

    auto overflow_record = reinterpret_cast<OverflowRecord*>(GetRecordPtr(slot_id));

    size_t length = slot.key_length;
    if (IsLeaf()) length += slot.value_length;

    auto page_size_ = page_size();
    auto page_count = length / page_size_;
    if (length % page_size_) ++page_count;

    uint32_t remaining_length = length;
    cached_record_.resize(length);
    if (page_count <= kMaxCachedPageCount) {
        for (uint32_t i = 0; i < page_count; i++) {
            auto page_ref = pager.Reference(overflow_record->pgid + i, false);
            auto page_buf = page_ref.page_buf();
            auto copy_length = std::min(static_cast<uint32_t>(page_size_), remaining_length);
            std::memcpy(&cached_record_[i * page_size_], page_buf, copy_length);
            remaining_length -= copy_length;
            if (i == 0) {
                cached_key_page_ = std::move(page_ref);
            }
        }
    } else {
        cached_key_page_ = pager.Reference(overflow_record->pgid, false);
        pager.ReadByBytes(overflow_record->pgid + 1, 0, reinterpret_cast<uint8_t*>(cached_record_.data()), length);
        std::memcpy(cached_record_.data(), cached_key_page_->page_buf(), page_size_);
    }
    cached_slot_id_ = slot_id;
}

void Node::StoreRecord(SlotId slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    if (key.size() > page_size()) {
        throw InvalidArgumentError("key length exceeds the limit.");
    }

    auto length = key.size() + value.size();
    auto& slot = struct_->slots[slot_id];
    slot.key_length = key.size();
    if (IsLeaf()) {
        slot.value_length = value.size();
    }

    // 需要创建overflow page存放
    if (SpaceNeeded(length) > MaxInlineRecordLength()) {
        auto pgid = StoreRecordToOverflowPages(slot_id, key, value);
        OverflowRecord record{ .pgid = pgid };
        struct_->header.data_offset -= sizeof(record);
        struct_->header.space_used += sizeof(record);
        slot.record_offset = struct_->header.data_offset;
        slot.is_overflow_pages = true;
        std::memcpy(GetRecordPtr(slot_id), &record, sizeof(record));
    } else {
        struct_->header.data_offset -= length;
        struct_->header.space_used += length;
        slot.record_offset = struct_->header.data_offset;
        slot.is_overflow_pages = false;
        std::memcpy(GetRecordPtr(slot_id), key.data(), key.size());
        if (!value.empty()) {
            std::memcpy(GetRecordPtr(slot_id) + key.size(), value.data(), value.size());
        }
    }
}

void Node::CopyRecordRange(Node* dst) {
    for (SlotId i = 0; i < struct_->header.count; ++i) {
        size_t length;
        auto& slot = struct_->slots[i];
        if (slot.is_overflow_pages) {
            length = sizeof(OverflowRecord);
        } else {
            length = slot.key_length;
            if (IsLeaf()) {
                length += slot.value_length;
            }
        }
        dst->struct_->header.data_offset -= length;
        dst->struct_->header.space_used += length;
        auto dst_ptr = dst->Ptr() + dst->struct_->header.data_offset;
        std::memcpy(dst_ptr, GetRecordPtr(i), length);
        slot.record_offset = dst->struct_->header.data_offset;
    }
}

void Node::DeleteRecord(SlotId slot_id) {
    assert(slot_id < count());
    auto& slot = struct_->slots[slot_id];
    if (slot.is_overflow_pages) {
        struct_->header.space_used -= sizeof(OverflowRecord);
    } else {
        struct_->header.space_used -= slot.key_length;
        slot.key_length = 0;
        if (IsLeaf()) {
            struct_->header.space_used -= slot.value_length;
            slot.value_length = 0;
        }
    }
}

void Node::RestoreRecord(SlotId slot_id, const Slot& saved_slot) {
    assert(slot_id < count());
    auto& slot = struct_->slots[slot_id];
    if (saved_slot.is_overflow_pages) {
        struct_->header.space_used += sizeof(OverflowRecord);
    } else {
        slot.key_length = saved_slot.key_length;
        struct_->header.space_used += slot.key_length;
        if (IsLeaf()) {
            slot.value_length = saved_slot.value_length;
            struct_->header.space_used += slot.value_length;
        }
    }
}


void BranchNode::Build(PageId tail_child) {
    auto& header = struct_->header;

    header.type = NodeType::kBranch;
    header.count = 0;

    header.data_offset = page_size();
    header.space_used = 0;

    struct_->tail_child = tail_child;
    header.last_modified_txid = btree_->bucket().tx().txid();
}

void BranchNode::Destroy() {

}

PageId BranchNode::GetLeftChild(SlotId slot_id) {
    assert(slot_id <= count());
    if (slot_id == count()) {
        return GetTailChild();
    }
    return struct_->slots[slot_id].left_child;
}

void BranchNode::SetLeftChild(SlotId slot_id, PageId child) {
    assert(slot_id <= count());
    if (slot_id == count()) {
        SetTailChild(child);
        return;
    }
    struct_->slots[slot_id].left_child = child;
}

PageId BranchNode::GetRightChild(SlotId slot_id) {
    assert(slot_id < count());
    if (slot_id == count() - 1) {
        return GetTailChild();
    }
    return struct_->slots[slot_id + 1].left_child;
}

void BranchNode::SetRightChild(uint16_t slot_id, PageId child) {
    assert(slot_id < count());
    if (slot_id == count() - 1) {
        SetTailChild(child);
        return;
    }
    struct_->slots[slot_id + 1].left_child = child;
}

PageId BranchNode::GetTailChild() {
    return struct_->tail_child;
}

void BranchNode::SetTailChild(PageId child) {
    struct_->tail_child = child;
}

bool BranchNode::Update(SlotId slot_id, std::span<const uint8_t> key, PageId child, bool is_right_child) {
    assert(slot_id < count());
    if (!Update(slot_id, key)) {
        return false;
    }
    auto& slot = struct_->slots[slot_id];
    if (is_right_child) {
        if (slot_id == count() - 1) {
            struct_->tail_child = child;
        } else {
            struct_->slots[slot_id + 1].left_child = child;
        }
    } else {
        slot.left_child = child;
    }
    return true;
}

bool BranchNode::Update(SlotId slot_id, std::span<const uint8_t> key) {
    assert(slot_id < count());
    auto saved_slot = struct_->slots[slot_id];
    DeleteRecord(slot_id);
    if (!RequestSpaceFor(key, {})) {
        // not enough space, restore deleted record.
        RestoreRecord(slot_id, saved_slot);
        return false;
    }
    auto& slot = struct_->slots[slot_id];
    auto length = slot.key_length;
    struct_->header.space_used -= length;
    StoreRecord(slot_id, key, {});
    assert(SlotSpace() + FreeSpace() == struct_->header.data_offset);
    return true;
}

bool BranchNode::Append(std::span<const uint8_t> key, PageId child, bool is_right_child) {
    return Insert(count(), key, child, is_right_child);
}

bool BranchNode::Insert(SlotId slot_id, std::span<const uint8_t> key, PageId child, bool is_right_child) {
    assert(slot_id <= count());
    if (!RequestSpaceFor(key, {})) {
        return false;
    }
    auto& slot = struct_->slots[slot_id];
    std::memmove(struct_->slots + slot_id + 1, struct_->slots + slot_id,
        sizeof(Slot) * (count() - slot_id));
    if (is_right_child) {
        if (slot_id == count()) {
            slot.left_child = struct_->tail_child;
            struct_->tail_child = child;
        } else {
            slot.left_child = struct_->slots[slot_id + 1].left_child;
            struct_->slots[slot_id + 1].left_child = child;
        }
    } else {
        slot.left_child = child;
    }
    ++struct_->header.count;
    StoreRecord(slot_id, key, {});
    assert(SlotSpace() + FreeSpace() == struct_->header.data_offset);
    return true;
}

void BranchNode::Delete(SlotId slot_id, bool right_child) {
    assert(slot_id < count());
    auto& slot = struct_->slots[slot_id];
    if (slot.is_overflow_pages) {
        struct_->header.space_used -= sizeof(OverflowRecord);
    } else {
        struct_->header.space_used -= slot.key_length;
    }
    if (right_child) {
        if (slot_id + 1 < count()) {
            struct_->slots[slot_id + 1].left_child = slot.left_child;
            slot = struct_->slots[slot_id + 1];
        } else {
            struct_->tail_child = struct_->slots[count() - 1].left_child;
        }
        if (count() - slot_id > 1) {
            std::memmove(struct_->slots + slot_id + 1, struct_->slots + slot_id + 2,
                sizeof(Slot) * (count() - slot_id - 2));
        }
    } else {
        std::memmove(struct_->slots + slot_id, struct_->slots + slot_id + 1,
            sizeof(Slot) * (count() - slot_id - 1));
    }
    --struct_->header.count;
    assert(SlotSpace() + FreeSpace() == struct_->header.data_offset);
}

void BranchNode::Pop(bool right_cbild) {
    Delete(count() - 1, right_cbild);
}


void LeafNode::Build() {
    auto& header = struct_->header;

    header.type = NodeType::kLeaf;
    header.count = 0;

    header.data_offset = page_size();
    header.space_used = 0;
    struct_->header.last_modified_txid = btree_->bucket().tx().txid();
}

void LeafNode::Destroy() {

}

Slot& LeafNode::GetSlot(SlotId slot_id) {
    return struct_->slots[slot_id];
}

std::span<const uint8_t> LeafNode::GetValue(SlotId slot_id) {
    assert(slot_id < count());
    auto& slot = struct_->slots[slot_id];
    if (!slot.is_overflow_pages) {
        return { GetRecordPtr(slot_id) + slot.key_length, slot.value_length }; 
    }
    if (cached_slot_id_ != slot_id) {
        LoadRecordFromOverflowPages(slot_id);
    }
    assert(cached_record_.size() == slot.key_length + slot.value_length);
    return { reinterpret_cast<const uint8_t*>(cached_record_.data()) + slot.key_length, slot.value_length };
    
}

bool LeafNode::Update(SlotId slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    assert(slot_id < count());
    auto saved_slot = struct_->slots[slot_id];
    DeleteRecord(slot_id);
    if (!RequestSpaceFor(key, value)) {
        // not enough space, restore deleted record.
        RestoreRecord(slot_id, saved_slot);
        return false;
    }
    auto& slot = struct_->slots[slot_id];
    StoreRecord(slot_id, key, value);
    assert(SlotSpace() + FreeSpace() == struct_->header.data_offset);
    return true;
}

bool LeafNode::Append(std::span<const uint8_t> key, std::span<const uint8_t> value) {
    return Insert(count(), key, value);
}

bool LeafNode::Insert(SlotId slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    assert(slot_id <= count());
    if (!RequestSpaceFor(key, value)) {
        return false;
    }
    std::memmove(struct_->slots + slot_id + 1, struct_->slots + slot_id,
        sizeof(Slot) * (count() - slot_id));
    ++struct_->header.count;
    StoreRecord(slot_id, key, value);
    assert(SlotSpace() + FreeSpace() == struct_->header.data_offset);
    return true;
}

void LeafNode::Delete(SlotId slot_id) {
    assert(slot_id < count());
    auto& slot = struct_->slots[slot_id];
    auto length = slot.key_length + slot.value_length;
    if (slot.is_overflow_pages) {
        struct_->header.space_used -= sizeof(OverflowRecord);
    } else {
        struct_->header.space_used -= length;
    }

    std::memmove(struct_->slots + slot_id, struct_->slots + slot_id + 1,
        sizeof(Slot) * (count() - slot_id - 1));
    --struct_->header.count;
    assert(SlotSpace() + FreeSpace() == struct_->header.data_offset);
}

void LeafNode::Pop() {
    Delete(count() - 1);
}

} // namespace yudb