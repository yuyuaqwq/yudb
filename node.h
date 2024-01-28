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
#include "page_reference.h"
#include "page_arena.h"

namespace yudb {

class BTree;

class Node : noncopyable {
public:
    using Iterator = NodeIterator;

    Node(const BTree* btree, PageId page_id, bool dirty);

    Node(const BTree* btree, PageReference page_ref);

    Node(Node&& right) noexcept;

    void operator=(Node&& right) noexcept;


    bool IsLeaf() const {
        return node_format_->type == NodeFormat::Type::kLeaf;
    }

    bool IsBranch() const {
        return node_format_->type == NodeFormat::Type::kBranch;
    }


    /*
    * 实现返回的可能是其中的一段(因为最多返回其中的一页)，需要循环才能读完
    * kPage类型建议另外实现一个传入buff的函数，直接读取到buff中
    */
    std::tuple<const uint8_t*, size_t, std::optional<PageReference>>
    CellLoad(const Cell& cell) {
        if (cell.type == Cell::Type::kEmbed) {
            return { cell.embed.data, cell.embed.size, std::nullopt };
        }
        else if (cell.type == Cell::Type::kBlock) {
            auto [buf, page] = block_manager_.Load(cell.block.record_index(), cell.block.offset);
            return { buf, cell.block.size, std::move(page) };
        }
        else {
            throw std::runtime_error("unrealized types.");
        }
    }

    size_t CellSize(const Cell& cell) {
        if (cell.type == Cell::Type::kEmbed) {
            return cell.embed.size;
        }
        else if (cell.type == Cell::Type::kBlock) {
            return cell.block.size;
        }
        else {
            throw std::runtime_error("unrealized types.");
        }
    }

    /*
    * 将数据保存到Node中
    */
    Cell CellAlloc(std::span<const uint8_t> data) {
        Cell cell;
        cell.bucket_flag = 0;
        if (data.size() <= sizeof(Cell::embed.data)) {
            cell.type = Cell::Type::kEmbed;
            cell.embed.size = data.size();
            std::memcpy(cell.embed.data, data.data(), data.size());
        }
        else if (data.size() <= block_manager_.MaxSize()) {
            auto res = block_manager_.Alloc(data.size());
            if (!res) {
                throw std::runtime_error("blocker alloc error.");
            }

            auto [index, offset] = *res;
            cell.type = Cell::Type::kBlock;
            cell.block.set_record_index(index);
            cell.block.offset = offset;
            cell.block.size = data.size();

            auto [buf, page] = block_manager_.Load(cell.block.record_index(), cell.block.offset);
            std::memcpy(buf, data.data(), data.size());

            //printf("alloc\n"); blocker_.Print(); printf("\n");
        }
        else {
            throw std::runtime_error("unrealized types.");
        }
        return cell;
    }

    void CellFree(Cell&& cell) {
        if (cell.type == Cell::Type::kInvalid) {}
        else if (cell.type == Cell::Type::kEmbed) {}
        else if (cell.type == Cell::Type::kBlock) {
            block_manager_.Free({ cell.block.record_index(), cell.block.offset, cell.block.size });
            //printf("free\n"); blocker_.Print(); printf("\n");
        }
        else {
            throw std::runtime_error("unrealized types.");
        }
        cell.type = Cell::Type::kInvalid;
    }

    /*
    * 将cell移动到新节点
    */
    Cell CellMove(Node* new_node, Cell&& cell) {
        auto [buf, size, ref] = CellLoad(cell);
        auto bucket_flag = cell.bucket_flag;
        auto new_cell = new_node->CellAlloc({ buf, size });
        new_cell.bucket_flag = bucket_flag;
        CellFree(std::move(cell));
        return new_cell;
    }

    /*
    * 将cell拷贝到新节点
    */
    Cell CellCopy(Node* new_node, const Cell& cell) {
        auto [buf, size, ref] = CellLoad(cell);
        auto bucket_flag = cell.bucket_flag;
        auto new_cell = new_node->CellAlloc({ buf, size });
        new_cell.bucket_flag = bucket_flag;
        return new_cell;
    }



    void CellClear();


    void BlockInit() {
        node_format_->block_table_descriptor.pgid = kPageInvalidId;
        node_format_->block_table_descriptor.count = 0;
    }

    void BlockPrint() {
        block_manager_.Print();
    }


    void LeafBuild() {
        BlockInit();
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
        BlockInit();
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



    const BTree& btree() const { return *btree_; }

    //const NodeFormat& node_format() const { return *node_format_; }

    //NodeFormat& node_format() { return *node_format_; }


    TxId last_modified_txid() const {
        return node_format_->last_modified_txid;
    }

    void set_last_modified_txid(TxId txid) {
        node_format_->last_modified_txid = txid;
    }

    uint16_t element_count() const {
        return node_format_->element_count;
    }

    PageId tail_child() const {
        assert(IsBranch());
        return node_format_->body.tail_child;
    }

    void set_tail_child(PageId pgid) {
        node_format_->body.tail_child = pgid;
    }

    const Cell& branch_key(size_t i) const {
        assert(IsBranch());
        return node_format_->body.branch[i].key;
    }

    Cell& branch_key(size_t i) {
        assert(IsBranch());
        return node_format_->body.branch[i].key;
    }

    void set_branch_key(size_t i, Cell&& key) {
        assert(IsBranch());
        node_format_->body.branch[i].key = std::move(key);
    }

    PageId branch_left_child(size_t i) const {
        assert(IsBranch());
        return node_format_->body.branch[i].left_child;
    }

    void set_branch_left_child(size_t i, PageId pgid) {
        assert(IsBranch());
        node_format_->body.branch[i].left_child = pgid;
    }

    const Cell& leaf_key(size_t i) const {
        assert(IsLeaf());
        return node_format_->body.leaf[i].key;
    }

    Cell& leaf_key(size_t i) {
        assert(IsLeaf());
        return node_format_->body.leaf[i].key;
    }

    const Cell& leaf_value(size_t i) const {
        assert(IsLeaf());
        return node_format_->body.leaf[i].value;
    }

    Cell& leaf_value(size_t i) {
        assert(IsLeaf());
        return node_format_->body.leaf[i].value;
    }

    void set_leaf_value(size_t i, Cell&& value) {
        assert(IsLeaf());
        node_format_->body.leaf[i].value = std::move(value);
    }

    const BlockTableDescriptor& block_table_descriptor() const {
        return node_format_->block_table_descriptor;
    }


    void LeafCheck();

    void BranchCheck();


    PageId page_id() const { return page_ref_.id(); }

    template <typename T>
    const T& page_content() const { return page_ref_.content<T>(); }

    template <typename T>
    T& page_content() { return page_ref_.content<T>(); }


protected:
    void PageSpaceBuild();

protected:
    const BTree* btree_;

    PageReference page_ref_;
    NodeFormat* node_format_;
    PageArena page_arena_;
    BlockManager block_manager_;
};

class ImmNode : Node {
public:
    ImmNode(const BTree* btree, PageId page_id) : Node{ btree, page_id, false } {}


    Iterator begin() { return Node::begin(); }

    Iterator end() { return Node::end(); }


    bool IsLeaf() const {
        return Node::IsLeaf();
    }

    bool IsBranch() const {
        return Node::IsBranch();
    }

    void BranchCheck() {
        Node::BranchCheck();
    }
    void LeafCheck() {
        Node::LeafCheck();
    }

    void BlockPrint() {
        return Node::BlockPrint();
    }

    //const NodeFormat& node_format() const { return Node::node_format(); }

    TxId last_modified_txid() const {
        return Node::last_modified_txid();
    }

    uint16_t element_count() const {
        return Node::element_count();
    }

    PageId tail_child() const {
        return Node::tail_child();
    }

    const Cell& branch_key(size_t i) const {
        return Node::branch_key(i);
    }

    const PageId branch_left_child(size_t i) const {
        return Node::branch_left_child(i);
    }

    const Cell& leaf_key(size_t i) const {
        return Node::leaf_key(i);
    }

    const Cell& leaf_value(size_t i) const {
        return Node::leaf_value(i);
    }

    std::tuple<const uint8_t*, size_t, std::optional<PageReference>>
    CellLoad(const Cell& cell){
        return Node::CellLoad(cell);
    }

    PageId BranchGetLeftChild(uint16_t pos) {
        return Node::BranchGetLeftChild(pos);
    }

    PageId BranchGetRightChild(uint16_t pos) {
        return Node::BranchGetRightChild(pos);
    }
};

class MutNode : public Node {
public:
    MutNode(const BTree* btree, PageId page_id) : Node{ btree, page_id, true } {}

    MutNode Copy() const;
};

} // namespace yudb