#pragma once

#include <cassert>

#include <span>
#include <stdexcept>
#include <optional>
#include <variant>
#include <vector>

#include "noncopyable.h"
#include "node_format.h"
#include "node_iterator.h"
#include "block_manager.h"
#include "page.h"
#include "page_arena.h"

namespace yudb {

class BTree;

class NodeImpl : noncopyable {
public:
    using Iterator = NodeIterator;

    NodeImpl(BTree* btree, PageId page_id, bool dirty);
    NodeImpl(BTree* btree, Page page_ref);
    ~NodeImpl() = default;

    NodeImpl(NodeImpl&& right) noexcept;
    void operator=(NodeImpl&& right) noexcept;

    bool IsLeaf() const { return node_format_->type == NodeFormat::Type::kLeaf; }
    bool IsBranch() const { return node_format_->type == NodeFormat::Type::kBranch; }

    std::tuple<const uint8_t*, uint32_t, std::optional<std::variant<ConstPage, std::string>>>
    CellLoad(const Cell& cell);
    size_t CellSize(const Cell& cell);
    Cell CellSave(std::span<const uint8_t> data);
    void CellFree(Cell&& cell);
    /*
    * 将cell移动到新节点
    */
    Cell CellMove(NodeImpl* new_node, Cell&& cell) {
        auto [buf, size, page] = CellLoad(cell);
        auto bucket_flag = cell.bucket_flag;
        auto new_cell = new_node->CellSave({ buf, size });
        new_cell.bucket_flag = bucket_flag;
        CellFree(std::move(cell));
        return new_cell;
    }
    /*
    * 将cell拷贝到新节点
    */
    Cell CellCopy(NodeImpl* new_node, const Cell& cell) {
        auto [buf, size, ref] = CellLoad(cell);
        auto bucket_flag = cell.bucket_flag;
        auto new_cell = new_node->CellSave({ buf, size });
        new_cell.bucket_flag = bucket_flag;
        return new_cell;
    }
    void CellClear();

    void BlockBuild() {
        node_format_->block_table_descriptor.pgid = kPageInvalidId;
        node_format_->block_table_descriptor.count = 0;
    }
    void BlockPrint() {
        block_manager_.Print();
    }

    void LeafBuild() {
        BlockBuild();
        PageSpaceBuild();
        node_format_->type = NodeFormat::Type::kLeaf;
        node_format_->element_count = 0;
    }
    void LeafAlloc(uint16_t ele_count);
    void LeafFree(uint16_t ele_count);
    // 不释放原来的key和value，需要提前分配
    void LeafSet(uint16_t pos, Cell&& key, Cell&& value) {
        node_format_->body.leaf[pos].key = std::move(key);
        node_format_->body.leaf[pos].value = std::move(value);
    }
    void LeafInsert(uint16_t pos, Cell&& key, Cell&& value) {
        assert(pos <= node_format_->element_count);
        LeafAlloc(1);
        std::memmove(&node_format_->body.leaf[pos + 1], &node_format_->body.leaf[pos], (node_format_->element_count - 1 - pos) * sizeof(node_format_->body.leaf[pos]));
        node_format_->body.leaf[pos].key = std::move(key);
        node_format_->body.leaf[pos].value = std::move(value);
    }
    void LeafDelete(uint16_t pos) {
        assert(pos < node_format_->element_count);
        CellFree(std::move(node_format_->body.leaf[pos].key));
        CellFree(std::move(node_format_->body.leaf[pos].value));
        std::memmove(&node_format_->body.leaf[pos], &node_format_->body.leaf[pos + 1], (node_format_->element_count - pos - 1) * sizeof(node_format_->body.leaf[pos]));
        LeafFree(1);
    }

    void BranchBuild() {
        BlockBuild();
        PageSpaceBuild();
        node_format_->type = NodeFormat::Type::kBranch;
        node_format_->element_count = 0;
    }
    void BranchAlloc(uint16_t ele_count);
    void BranchFree(uint16_t ele_count);
    void BranchInsert(uint16_t pos, Cell&& key, PageId child, bool right_child) {
        assert(pos <= node_format_->element_count);
        auto original_count = node_format_->element_count;
        BranchAlloc(1);
        if (right_child) {
            if (pos == original_count) {
                node_format_->body.branch[pos].left_child = node_format_->body.tail_child;
                node_format_->body.tail_child = child;
            }
            else {
                std::memmove(&node_format_->body.branch[pos + 1], &node_format_->body.branch[pos], (original_count - pos) * sizeof(node_format_->body.branch[pos]));
                node_format_->body.branch[pos].left_child = node_format_->body.branch[pos + 1].left_child;
                node_format_->body.branch[pos + 1].left_child = child;
            }
        }
        else {
            std::memmove(&node_format_->body.branch[pos + 1], &node_format_->body.branch[pos], (original_count - pos) * sizeof(node_format_->body.branch[pos]));
            node_format_->body.branch[pos].left_child = child;
        }
        node_format_->body.branch[pos].key = std::move(key);
    }
    void BranchDelete(uint16_t pos, bool right_child) {
        assert(pos < node_format_->element_count);
        auto copy_count = node_format_->element_count - pos - 1;
        if (right_child) {
            if (pos + 1 < node_format_->element_count) {
                node_format_->body.branch[pos].key = std::move(node_format_->body.branch[pos + 1].key);
            }
            else {
                node_format_->body.tail_child = node_format_->body.branch[node_format_->element_count - 1].left_child;
            }
            if (copy_count > 0) {
                CellFree(std::move(node_format_->body.branch[pos + 1].key));
                std::memmove(&node_format_->body.branch[pos + 1], &node_format_->body.branch[pos + 2], (copy_count - 1) * sizeof(node_format_->body.branch[pos]));
            }
        }
        else {
            CellFree(std::move(node_format_->body.branch[pos].key));
            std::memmove(&node_format_->body.branch[pos], &node_format_->body.branch[pos + 1], copy_count * sizeof(node_format_->body.branch[pos]));
        }
        BranchFree(1);
    }
    /*
    * 不处理tail_child的关系，不释放原来的key，需要自己保证tail_child的正确性
    */
    void BranchSet(uint16_t pos, Cell&& key, PageId left_child) {
        node_format_->body.branch[pos].key = std::move(key);
        node_format_->body.branch[pos].left_child = left_child;
    }
    PageId BranchGetLeftChild(uint16_t pos) {
        assert(pos <= node_format_->element_count);
        if (pos == node_format_->element_count) {
            return node_format_->body.tail_child;
        }
        return node_format_->body.branch[pos].left_child;
    }
    PageId BranchGetRightChild(uint16_t pos) {
        assert(pos < node_format_->element_count);
        if (pos == node_format_->element_count - 1) {
            return node_format_->body.tail_child;
        }
        return node_format_->body.branch[pos + 1].left_child;
    }
    void BranchSetLeftChild(uint16_t pos, PageId left_child) {
        assert(pos <= node_format_->element_count);
        if (pos == node_format_->element_count) {
            node_format_->body.tail_child = left_child;
        }
        node_format_->body.branch[pos].left_child = left_child;
    }
    void BranchSetRightChild(uint16_t pos, PageId right_child) {
        assert(pos < node_format_->element_count);
        if (pos == node_format_->element_count - 1) {
            node_format_->body.tail_child = right_child;
        }
        node_format_->body.branch[pos + 1].left_child = right_child;
    }

    Iterator begin() { return Iterator{ node_format_, 0 }; }
    Iterator end() { return Iterator{ node_format_, node_format_->element_count }; }

    void LeafCheck() const;
    void BranchCheck() const;

    void DefragmentSpace(uint16_t index);
    void BlockRealloc(BlockPage* new_page, uint16_t index, Cell* cell);


    auto& btree() const { return *btree_; }
    auto& last_modified_txid() const { return node_format_->last_modified_txid; }
    void set_last_modified_txid(TxId txid) { node_format_->last_modified_txid = txid; }
    auto element_count() const { return node_format_->element_count; }
    auto& tail_child() const { assert(IsBranch()); return node_format_->body.tail_child; }
    void set_tail_child(PageId pgid) { assert(IsBranch()); node_format_->body.tail_child = pgid; }
    auto& branch_key(size_t i) const { assert(IsBranch()); return node_format_->body.branch[i].key; }
    auto& branch_key(size_t i) { assert(IsBranch()); return node_format_->body.branch[i].key; }
    void set_branch_key(size_t i, Cell&& key) { assert(IsBranch()); node_format_->body.branch[i].key = std::move(key); }
    auto& branch_left_child(size_t i) const { assert(IsBranch()); return node_format_->body.branch[i].left_child; }
    void set_branch_left_child(size_t i, PageId pgid) { assert(IsBranch()); node_format_->body.branch[i].left_child = pgid; }
    auto& leaf_key(size_t i) const { assert(IsLeaf()); return node_format_->body.leaf[i].key; }
    auto& leaf_key(size_t i) { assert(IsLeaf()); return node_format_->body.leaf[i].key; }
    auto& leaf_value(size_t i) const { assert(IsLeaf()); return node_format_->body.leaf[i].value; }
    auto& leaf_value(size_t i) { assert(IsLeaf()); return node_format_->body.leaf[i].value; }
    void set_leaf_value(size_t i, Cell&& value) { assert(IsLeaf()); node_format_->body.leaf[i].value = std::move(value); }
    auto& block_table_descriptor() const { return node_format_->block_table_descriptor; }
    PageId page_id() const { return page_.page_id(); }
    template <typename T> const T& page_content() const { return page_.content<T>(); }
    template <typename T> T& page_content() { return page_.content<T>(); }

protected:
    void PageSpaceBuild();

protected:
    BTree* const btree_;

    Page page_;
    NodeFormat* node_format_;
    PageArena page_arena_;
    BlockManager block_manager_;
};

class ConstNode {
public:
    ConstNode(BTree* btree, PageId page_id) : node_{ btree, page_id, false } {}

    NodeImpl::Iterator begin() { return node_.begin(); }
    NodeImpl::Iterator end() { return node_.end(); }

    bool IsLeaf() const {
        return node_.IsLeaf();
    }
    bool IsBranch() const {
        return node_.IsBranch();
    }
    void BranchCheck() const {
        node_.BranchCheck();
    }
    void LeafCheck() const {
        node_.LeafCheck();
    }

    void BlockPrint() {
        return node_.BlockPrint();
    }

    std::tuple<const uint8_t*, uint32_t, std::optional<std::variant<ConstPage, std::string>>>
    CellLoad(const Cell& cell){
        return node_.CellLoad(cell);
    }

    PageId BranchGetLeftChild(uint16_t pos) {
        return node_.BranchGetLeftChild(pos);
    }
    PageId BranchGetRightChild(uint16_t pos) {
        return node_.BranchGetRightChild(pos);
    }


    TxId last_modified_txid() const {
        return node_.last_modified_txid();
    }
    uint16_t element_count() const {
        return node_.element_count();
    }
    PageId tail_child() const {
        return node_.tail_child();
    }
    const Cell& branch_key(size_t i) const {
        return node_.branch_key(i);
    }
    const PageId branch_left_child(size_t i) const {
        return node_.branch_left_child(i);
    }
    const Cell& leaf_key(size_t i) const {
        return node_.leaf_key(i);
    }
    const Cell& leaf_value(size_t i) const {
        return node_.leaf_value(i);
    }

private:
    NodeImpl node_;
};

class Node : public NodeImpl {
public:
    Node(BTree* btree, PageId page_id) : Node{ btree, page_id, true } {}
    Node(BTree* btree, PageId page_id, bool try_defragment_space) : NodeImpl{ btree, page_id, true }
    {
        if (try_defragment_space) {
            block_manager_.TryDefragmentSpace();
        }
    }

    Node Copy() const;
};

} // namespace yudb