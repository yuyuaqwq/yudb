#include "node.h"

#include "bucket_impl.h"
#include "pager.h"
#include "tx.h"

namespace yudb {

constexpr PageCount kMaxCachedPageCount = 32;

Node::Node(BTree* btree, PageId page_id, bool dirty) :
    btree_{ btree },
    page_{ btree_->bucket().pager().Reference(page_id, dirty) },
    header_{ reinterpret_cast<NodeHeader*>(page_->page_buf()) } {}
Node::Node(BTree* btree, Page page_ref) :
    btree_{ btree },
    page_{ std::move(page_ref) },
    header_{ reinterpret_cast<NodeHeader*>(page_->page_buf()) } {}
Node::Node(BTree* btree, uint8_t* page_buf) :
    btree_{ btree },
    header_{ reinterpret_cast<NodeHeader*>(page_buf) } {}
Node::~Node() = default;

Node::Node(Node&& right) noexcept : 
    btree_{ right.btree_ },
    page_{ std::move(right.page_) },
    header_{ reinterpret_cast<NodeHeader*>(page_->page_buf()) } {}

void Node::operator=(Node&& right) noexcept {
    assert(btree_ == right.btree_);
    page_ = std::move(right.page_);
    header_ = reinterpret_cast<NodeHeader*>(page_->page_buf());
}

bool Node::IsLeaf() const {
    return header_->type == NodeType::kLeaf;
}
bool Node::IsBranch() const {
    return header_->type == NodeType::kBranch;
}

Node Node::Copy() const {
    auto& pager = btree_->bucket().pager();
    auto& tx = btree_->bucket().tx();
    assert(page_.has_value());
    Node node{ btree_, pager.Copy(*page_) };
    node.set_last_modified_txid(tx.txid());
    return node;
}


Page Node::Release() {
    assert(page_.has_value());
    return std::move(*page_);
}

PageId Node::page_id() const {
    return page_->page_id();
}
TxId Node::last_modified_txid() const {
    return header_->last_modified_txid;
}
 
uint16_t Node::count() {
    return header_->count;
}


size_t Node::SpaceNeeded(size_t record_length) {
    return record_length + sizeof(Slot);
}


size_t Node::SlotSpace(Slot* slots) {
    auto slot_space = reinterpret_cast<const uint8_t*>(slots + header_->count) - Ptr();
    assert(slot_space < page_size());
    return slot_space;
}

size_t Node::FreeSpace(Slot* slots) {
    auto free_space = header_->data_offset - SlotSpace(slots);
    assert(free_space < page_size());
    return free_space;
}

size_t Node::FreeSpaceAfterCompaction(Slot* slots) {
    auto free_space = page_size() - SlotSpace(slots) - header_->space_used;
    assert(free_space < page_size());
    return free_space;
}

void Node::Compactify(Slot* slots) {
    auto& pager = btree_->bucket().pager();
    auto& tmp_page = pager.tmp_page();
    Node tmp_node{ btree_, tmp_page };
    std::memset(tmp_node.header_, 0, sizeof(*tmp_node.header_));

    tmp_node.header_->data_offset = pager.page_size();
    CopyRecordRange(&tmp_node, slots);
    assert(header_->space_used == tmp_node.header_->space_used);
    header_->data_offset = tmp_node.header_->data_offset;
    std::memcpy(Ptr() + header_->data_offset, 
        tmp_node.Ptr() + tmp_node.header_->data_offset,
        tmp_node.header_->space_used);
}

void Node::CopyRecordRange(Node* dst, Slot* src_slots) {
    for (SlotId i = 0; i < header_->count; ++i) {
        size_t length;
        length = src_slots[i].key_length;
        if (IsLeaf()) {
            length += src_slots[i].value_length;
        }
        dst->header_->data_offset -= length;
        dst->header_->space_used += length;
        auto dst_ptr = dst->Ptr() + dst->header_->data_offset;
        std::memcpy(dst_ptr, GetRecordPtr(src_slots, i), length);
        src_slots[i].record_offset = dst->header_->data_offset;
    }
}

uint8_t* Node::Ptr() {
    return reinterpret_cast<uint8_t*>(header_);
}

uint8_t* Node::GetRecordPtr(Slot* slots, SlotId slot_id) {
    assert(slot_id < count()); 
    return Ptr() + slots[slot_id].record_offset;
}

PageSize Node::page_size() const {
    return btree_->bucket().pager().page_size();
}


void BranchNode::Build(PageId tail_child) {
    auto& header = node()->header;

    header.type = NodeType::kBranch;
    header.count = 0;

    header.data_offset = page_size();
    header.space_used = 0;

    node()->tail_child = tail_child;
    header_->last_modified_txid = btree_->bucket().tx().txid();
}

void BranchNode::Destroy() {

}

std::span<const uint8_t> BranchNode::GetKey(SlotId slot_id) {
    assert(slot_id < count());
    auto& slot = slots()[slot_id];
    if (slot.overflow_page) {

    }
    return { GetKeyPtr(slot_id), slot.key_length };
}

PageId BranchNode::GetLeftChild(SlotId slot_id) {
    assert(slot_id <= count());
    if (slot_id == count()) {
        return GetTailChild();
    }
    return slots()[slot_id].left_child;
}
void BranchNode::SetLeftChild(SlotId slot_id, PageId child) {
    assert(slot_id <= count());
    if (slot_id == count()) {
        SetTailChild(child);
        return;
    }
    slots()[slot_id].left_child = child;
}
PageId BranchNode::GetRightChild(SlotId slot_id) {
    assert(slot_id < count());
    if (slot_id == count() - 1) {
        return GetTailChild();
    }
    return slots()[slot_id + 1].left_child;
}
void BranchNode::SetRightChild(uint16_t slot_id, PageId child) {
    assert(slot_id < count());
    if (slot_id == count() - 1) {
        SetTailChild(child);
        return;
    }
    slots()[slot_id + 1].left_child = child;
}

PageId BranchNode::GetTailChild() {
    return node()->tail_child;
}
void BranchNode::SetTailChild(PageId child) {
    node()->tail_child = child;
}

std::pair<SlotId, bool> BranchNode::LowerBound(std::span<const uint8_t> key) {
    bool eq = false;
    auto res = std::lower_bound(slots(), slots() + count(), key, [&](const Slot& slot, std::span<const uint8_t> search_key) -> bool {
        SlotId slot_id = &slot - slots();
        auto slot_key = GetKey(slot_id);
        auto res = btree_->comparator()(slot_key, search_key);
        if (res == 0 && slot_key.size() != search_key.size()) {
            return slot_key.size() < search_key.size();
        }
        if (res != 0) {
            return res < 0;
        } else {
            eq = true;
            return false;
        }
    });
    return { res - slots(), eq };
}

bool BranchNode::Update(SlotId slot_id, std::span<const uint8_t> key, PageId child, bool is_right_child) {
    assert(slot_id < count());
    if (!Update(slot_id, key)) {
        return false;
    }
    auto& slot = slots()[slot_id];
    if (is_right_child) {
        if (slot_id == count() - 1) {
            node()->tail_child = child;
        } else {
            slots()[slot_id + 1].left_child = child;
        }
    } else {
        slot.left_child = child;
    }
    return true;
}

bool BranchNode::Update(SlotId slot_id, std::span<const uint8_t> key) {
    assert(slot_id < count());
    auto saved_slot = slots()[slot_id];
    DeleteRecord(slot_id);
    if (!RequestSpaceFor(key)) {
        // not enough space, restore deleted record.
        RestoreRecord(slot_id, saved_slot);
        return false;
    }
    auto& slot = slots()[slot_id];
    auto length = slot.key_length;
    header_->space_used -= length;
    StoreRecord(slot_id, key);
    assert(SlotSpace(slots()) + FreeSpace(slots()) == header_->data_offset);
    return true;
}

bool BranchNode::Append(std::span<const uint8_t> key, PageId child, bool is_right_child) {
    return Insert(count(), key, child, is_right_child);
}

bool BranchNode::Insert(SlotId slot_id, std::span<const uint8_t> key, PageId child, bool is_right_child) {
    assert(slot_id <= count());
    if (!RequestSpaceFor(key)) {
        return false;
    }
    auto& slot = slots()[slot_id];
    std::memmove(slots() + slot_id + 1, slots() + slot_id,
        sizeof(Slot) * (count() - slot_id));
    if (is_right_child) {
        if (slot_id == count()) {
            slot.left_child = node()->tail_child;
            node()->tail_child = child;
        } else {
            slot.left_child = slots()[slot_id + 1].left_child;
            slots()[slot_id + 1].left_child = child;
        }
    } else {
        slot.left_child = child;
    }
    ++header_->count;
    StoreRecord(slot_id, key);
    assert(SlotSpace(slots()) + FreeSpace(slots()) == header_->data_offset);
    return true;
}

void BranchNode::Delete(SlotId slot_id, bool right_child) {
    assert(slot_id < count());
    header_->space_used -= slots()[slot_id].key_length;
    if (right_child) {
        if (slot_id + 1 < count()) {
            slots()[slot_id + 1].left_child = slots()[slot_id].left_child;
            slots()[slot_id] = slots()[slot_id + 1];
        } else {
            node()->tail_child = slots()[count() - 1].left_child;
        }
        if (count() - slot_id > 1) {
            std::memmove(slots() + slot_id + 1, slots() + slot_id + 2,
                sizeof(Slot) * (count() - slot_id - 2));
        }
    } else {
        std::memmove(slots() + slot_id, slots() + slot_id + 1,
            sizeof(Slot) * (count() - slot_id - 1));
    }
    --header_->count;
    assert(SlotSpace(slots()) + FreeSpace(slots()) == header_->data_offset);
}

void BranchNode::Pop(bool right_cbild) {
    Delete(count() - 1, right_cbild);
}


size_t BranchNode::GetFillRate() {
    auto free_space = FreeSpaceAfterCompaction(slots());
    return (page_size() - free_space) * 100 / page_size();
}


BranchNodeFormat* BranchNode::node() {
    return reinterpret_cast<BranchNodeFormat*>(header_);
}
Slot* BranchNode::slots() {
    return node()->slots;
}



bool BranchNode::RequestSpaceFor(std::span<const uint8_t> key) {
    auto space_needed = SpaceNeeded(key.size());
    if (space_needed > MaxInlineRecordLength()) {
        space_needed = sizeof(OverflowRecord);
    }
    if (space_needed <= FreeSpace(slots())) {
        return true;
    }
    if (space_needed <= FreeSpaceAfterCompaction(slots())) {
        Compactify(slots());
        return true;
    }
    return false;
}

uint8_t* BranchNode::GetKeyPtr(SlotId slot_id) {
    return GetRecordPtr(slots(), slot_id);
}
void BranchNode::StoreRecord(SlotId slot_id, std::span<const uint8_t> key) {
    assert(slot_id < count());
    auto& slot = slots()[slot_id];
    slot.key_length = key.size();
    header_->data_offset -= key.size();
    header_->space_used += key.size();
    slot.record_offset = header_->data_offset;

    //需要创建overflow page存放
    if (key.size() > MaxInlineRecordLength()) {
        throw std::runtime_error("todo.");
    }
    else {
        std::memcpy(GetKeyPtr(slot_id), key.data(), key.size());
    }
}
void BranchNode::DeleteRecord(SlotId slot_id) {
    assert(slot_id < count());
    auto& slot = slots()[slot_id];
    header_->space_used -= slot.key_length;
    slot.key_length = 0;
}
void BranchNode::RestoreRecord(SlotId slot_id, const Slot& saved_slot) {
    assert(slot_id < count());
    auto& slot = slots()[slot_id];
    slot.key_length = saved_slot.key_length;
    header_->space_used += slot.key_length;
}
size_t BranchNode::MaxInlineRecordLength() {
    auto max_records_length = page_size() -
        sizeof(BranchNodeFormat::header) -
        sizeof(BranchNodeFormat::tail_child) -
        (sizeof(Slot) * 2);
    return max_records_length / 2;
}



void LeafNode::Build() {
    auto& header = node()->header;

    header.type = NodeType::kLeaf;
    header.count = 0;

    header.data_offset = page_size();
    header.space_used = 0;
    header_->last_modified_txid = btree_->bucket().tx().txid();
}
void LeafNode::Destroy() {

}

Slot& LeafNode::GetSlot(SlotId slot_id) {
    return slots()[slot_id];
}

std::span<const uint8_t> LeafNode::GetKey(SlotId slot_id) {
    assert(slot_id < count());
    auto& slot = slots()[slot_id];
    if (slot.overflow_page) {

    }
    return { GetKeyPtr(slot_id), slot.key_length };
}

std::span<const uint8_t> LeafNode::GetValue(SlotId slot_id) {
    assert(slot_id < count());
    auto& slot = slots()[slot_id];
    if (slot.overflow_page) {

    }
    return { GetValuePtr(slot_id), slot.value_length };
}


std::pair<SlotId, bool> LeafNode::LowerBound(std::span<const uint8_t> key) {
    bool eq = false;
    auto res = std::lower_bound(slots(), slots() + count(), key, [&](const Slot& slot, std::span<const uint8_t> search_key) -> bool {
        SlotId slot_id = &slot - slots();
        auto slot_key = GetKey(slot_id);
        auto res = btree_->comparator()(slot_key, search_key);
        if (res == 0 && slot_key.size() != search_key.size()) {
            return slot_key.size() < search_key.size();
        }
        if (res != 0) {
            return res < 0;
        } else {
            eq = true;
            return false;
        }
    });
    return { res - slots(), eq };
}

bool LeafNode::Update(SlotId slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    assert(slot_id < count());
    auto saved_slot = slots()[slot_id];
    DeleteRecord(slot_id);
    if (!RequestSpaceFor(key, value)) {
        // not enough space, restore deleted record.
        RestoreRecord(slot_id, saved_slot);
        return false;
    }
    auto& slot = slots()[slot_id];
    StoreRecord(slot_id, key, value);
    assert(SlotSpace(slots()) + FreeSpace(slots()) == header_->data_offset);
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
    std::memmove(slots() + slot_id + 1, slots() + slot_id,
        sizeof(Slot) * (count() - slot_id));
    ++header_->count;
    StoreRecord(slot_id, key, value);
    assert(SlotSpace(slots()) + FreeSpace(slots()) == header_->data_offset);
    return true;
}

void LeafNode::Delete(SlotId slot_id) {
    assert(slot_id < count());
    header_->space_used -= slots()[slot_id].key_length;
    header_->space_used -= slots()[slot_id].value_length;
    std::memmove(slots() + slot_id, slots() + slot_id + 1,
        sizeof(Slot) * (count() - slot_id - 1));
    --header_->count;
    assert(SlotSpace(slots()) + FreeSpace(slots()) == header_->data_offset);
}

void LeafNode::Pop() {
    Delete(count() - 1);
}


size_t LeafNode::GetFillRate() {
    auto free_space = FreeSpaceAfterCompaction(slots());
    return (page_size() - free_space) * 100 / page_size();
}




BranchNodeFormat* LeafNode::node() {
    return reinterpret_cast<BranchNodeFormat*>(header_);
}
Slot* LeafNode::slots() {
    return node()->slots;
}

bool LeafNode::RequestSpaceFor(std::span<const uint8_t> key, std::span<const uint8_t> value) {
    auto length = key.size() + value.size();
    auto space_needed = SpaceNeeded(length);
    if (space_needed > MaxInlineRecordLength()) {
        space_needed = SpaceNeeded(key.size() + sizeof(OverflowRecord));
        throw std::runtime_error("todo.");
    }
    if (space_needed <= FreeSpace(slots())) {
        return true;
    }
    if (space_needed <= FreeSpaceAfterCompaction(slots())) {
        Compactify(slots());
        return true;
    }
    return false;
}

uint8_t* LeafNode::GetKeyPtr(SlotId slot_id) {
    return GetRecordPtr(slots(), slot_id);
}
uint8_t* LeafNode::GetValuePtr(SlotId slot_id) {
    return GetRecordPtr(slots(), slot_id) + slots()[slot_id].key_length;
}

void LeafNode::StoreRecord(SlotId slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    auto& slot = slots()[slot_id];
    slot.key_length = key.size();
    slot.value_length = value.size();
    auto length = key.size() + value.size();
    header_->data_offset -= length;
    header_->space_used += length;
    slot.record_offset = header_->data_offset;
    slot.overflow_page = false;

    // 需要创建overflow page存放
    if (length > MaxInlineRecordLength()) {
        throw std::runtime_error("todo.");
    }
    else {
        std::memcpy(GetKeyPtr(slot_id), key.data(), key.size());
        std::memcpy(GetValuePtr(slot_id), value.data(), value.size());
    }

}
void LeafNode::DeleteRecord(SlotId slot_id) {
    auto& slot = slots()[slot_id];
    header_->space_used -= slot.key_length + slot.value_length;
    slot.key_length = 0;
    slot.value_length = 0;
}
void LeafNode::RestoreRecord(SlotId slot_id, const Slot& saved_slot) {
    auto& slot = slots()[slot_id];
    slot.key_length = saved_slot.key_length;
    slot.value_length = saved_slot.value_length;
    header_->space_used += slot.key_length + slot.value_length;
}


size_t LeafNode::MaxInlineRecordLength() {
    // ensure that each page can load at least 2 records.
    auto max_records_length = page_size() -
        sizeof(BranchNodeFormat::header) -
        sizeof(BranchNodeFormat::tail_child) -
        (sizeof(Slot) * 2);
    return max_records_length / 2;
}

} // namespace yudb