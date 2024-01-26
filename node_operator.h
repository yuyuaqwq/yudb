#pragma once

#include <cassert>

#include <span>
#include <stdexcept>
#include <optional>
#include <variant>
#include <vector>

#include "noncopyable.h"
#include "node.h"
#include "node_iterator.h"
#include "block_manager.h"
#include "page_reference.h"
#include "page_space_operator.h"

namespace yudb {

class BTree;

class NodeOperator : noncopyable {
public:
    using Iterator = NodeIterator;

    NodeOperator(const BTree* btree, PageId page_id, bool dirty);

    NodeOperator(const BTree* btree, PageReference page_ref);

    NodeOperator(NodeOperator&& right) noexcept;

    void operator=(NodeOperator&& right) noexcept;


    bool IsLeaf() const {
        return node_->type == Node::Type::kLeaf;
    }

    bool IsBranch() const {
        return node_->type == Node::Type::kBranch;
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
            auto [buf, page] = blocker_.Load(cell.block.record_index(), cell.block.offset);
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
        else if (data.size() <= blocker_.MaxSize()) {
            auto res = blocker_.Alloc(data.size());
            if (!res) {
                throw std::runtime_error("blocker alloc error.");
            }

            auto [index, offset] = *res;
            cell.type = Cell::Type::kBlock;
            cell.block.set_record_index(index);
            cell.block.offset = offset;
            cell.block.size = data.size();

            auto [buf, page] = blocker_.Load(cell.block.record_index(), cell.block.offset);
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
            blocker_.Free({ cell.block.record_index(), cell.block.offset, cell.block.size });
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
    Cell CellMove(NodeOperator* new_node, Cell&& cell) {
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
    Cell CellCopy(NodeOperator* new_node, const Cell& cell) {
        auto [buf, size, ref] = CellLoad(cell);
        auto bucket_flag = cell.bucket_flag;
        auto new_cell = new_node->CellAlloc({ buf, size });
        new_cell.bucket_flag = bucket_flag;
        return new_cell;
    }



    void CellClear();


    void BlockInit() {
        node_->block_info.pgid = kPageInvalidId;
        node_->block_info.count = 0;
    }

    void BlockPrint() {
        blocker_.Print();
    }


    void LeafBuild() {
        BlockInit();
        PageSpaceInit();
        node_->type = Node::Type::kLeaf;
        node_->element_count = 0;
    }

    void LeafAlloc(uint16_t ele_count);

    void LeafFree(uint16_t ele_count);

    // 不释放原来的key和value，需要提前分配
    void LeafSet(uint16_t pos, Cell&& key, Cell&& value) {
        node_->body.leaf[pos].key = std::move(key);
        node_->body.leaf[pos].value = std::move(value);
    }

    void LeafInsert(uint16_t pos, Cell&& key, Cell&& value) {
        assert(pos <= node_->element_count);
        LeafAlloc(1);
        std::memmove(&node_->body.leaf[pos + 1], &node_->body.leaf[pos], (node_->element_count - 1 - pos) * sizeof(node_->body.leaf[pos]));
        node_->body.leaf[pos].key = std::move(key);
        node_->body.leaf[pos].value = std::move(value);
    }

    void LeafDelete(uint16_t pos) {
        assert(pos < node_->element_count);
        CellFree(std::move(node_->body.leaf[pos].key));
        CellFree(std::move(node_->body.leaf[pos].value));
        std::memmove(&node_->body.leaf[pos], &node_->body.leaf[pos + 1], (node_->element_count - pos - 1) * sizeof(node_->body.leaf[pos]));
        LeafFree(1);
    }


    void BranchBuild() {
        BlockInit();
        PageSpaceInit();
        node_->type = Node::Type::kBranch;
        node_->element_count = 0;
    }

    void BranchAlloc(uint16_t ele_count);

    void BranchFree(uint16_t ele_count);

    void BranchInsert(uint16_t pos, Cell&& key, PageId child, bool right_child) {
        assert(pos <= node_->element_count);
        auto original_count = node_->element_count;
        BranchAlloc(1);
        if (right_child) {
            if (pos == original_count) {
                node_->body.branch[pos].left_child = node_->body.tail_child;
                node_->body.tail_child = child;
            }
            else {
                std::memmove(&node_->body.branch[pos + 1], &node_->body.branch[pos], (original_count - pos) * sizeof(node_->body.branch[pos]));
                node_->body.branch[pos].left_child = node_->body.branch[pos + 1].left_child;
                node_->body.branch[pos + 1].left_child = child;
            }
        }
        else {
            std::memmove(&node_->body.branch[pos + 1], &node_->body.branch[pos], (original_count - pos) * sizeof(node_->body.branch[pos]));
            node_->body.branch[pos].left_child = child;
        }
        node_->body.branch[pos].key = std::move(key);
    }

    void BranchDelete(uint16_t pos, bool right_child) {
        assert(pos < node_->element_count);
        auto copy_count = node_->element_count - pos - 1;
        if (right_child) {
            if (pos + 1 < node_->element_count) {
                node_->body.branch[pos].key = std::move(node_->body.branch[pos + 1].key);
            }
            else {
                node_->body.tail_child = node_->body.branch[node_->element_count - 1].left_child;
            }
            if (copy_count > 0) {
                CellFree(std::move(node_->body.branch[pos + 1].key));
                std::memmove(&node_->body.branch[pos + 1], &node_->body.branch[pos + 2], (copy_count - 1) * sizeof(node_->body.branch[pos]));
            }
        }
        else {
            CellFree(std::move(node_->body.branch[pos].key));
            std::memmove(&node_->body.branch[pos], &node_->body.branch[pos + 1], copy_count * sizeof(node_->body.branch[pos]));
        }
        BranchFree(1);
    }

    /*
    * 不处理tail_child的关系，不释放原来的key，需要自己保证tail_child的正确性
    */
    void BranchSet(uint16_t pos, Cell&& key, PageId left_child) {
        node_->body.branch[pos].key = std::move(key);
        node_->body.branch[pos].left_child = left_child;
    }

    PageId BranchGetLeftChild(uint16_t pos) {
        assert(pos <= node_->element_count);
        if (pos == node_->element_count) {
            return node_->body.tail_child;
        }
        return node_->body.branch[pos].left_child;
    }

    PageId BranchGetRightChild(uint16_t pos) {
        assert(pos < node_->element_count);
        if (pos == node_->element_count - 1) {
            return node_->body.tail_child;
        }
        return node_->body.branch[pos + 1].left_child;
    }

    void BranchSetLeftChild(uint16_t pos, PageId left_child) {
        assert(pos <= node_->element_count);
        if (pos == node_->element_count) {
            node_->body.tail_child = left_child;
        }
        node_->body.branch[pos].left_child = left_child;
    }

    void BranchSetRightChild(uint16_t pos, PageId right_child) {
        assert(pos < node_->element_count);
        if (pos == node_->element_count - 1) {
            node_->body.tail_child = right_child;
        }
        node_->body.branch[pos + 1].left_child = right_child;
    }




    Iterator begin() { return Iterator{ &node(), 0 }; }

    Iterator end() { return Iterator{ &node(), node_->element_count }; }



    const BTree& btree() const { return *btree_; }

    const Node& node() const { return *node_; }

    Node& node() { return *node_; }

    PageId page_id() const { return page_ref_.page_id(); }

    template <typename T>
    const T& page_content() const { return page_ref_.page_content<T>(); }

    template <typename T>
    T& page_content() { return page_ref_.page_content<T>(); }


    void LeafCheck();

    void BranchCheck();


protected:
    void PageSpaceInit();

protected:
    const BTree* btree_;

    PageReference page_ref_;
    Node* node_;
    PageSpaceOperator page_spacer_;
    BlockManager blocker_;
};

class ImmNodeOperator : NodeOperator {
public:
    ImmNodeOperator(const BTree* btree, PageId page_id) : NodeOperator{ btree, page_id, false } {}


    Iterator begin() { return NodeOperator::begin(); }

    Iterator end() { return NodeOperator::end(); }


    bool IsLeaf() const {
        return NodeOperator::IsLeaf();
    }

    bool IsBranch() const {
        return NodeOperator::IsBranch();
    }

    void BranchCheck() {
        NodeOperator::BranchCheck();
    }
    void LeafCheck() {
        NodeOperator::LeafCheck();
    }

    void BlockPrint() {
        return NodeOperator::BlockPrint();
    }

    const Node& node() const { return NodeOperator::node(); }

    std::tuple<const uint8_t*, size_t, std::optional<PageReference>>
    CellLoad(const Cell& cell){
        return NodeOperator::CellLoad(cell);
    }

    PageId BranchGetLeftChild(uint16_t pos) {
        return NodeOperator::BranchGetLeftChild(pos);
    }

    PageId BranchGetRightChild(uint16_t pos) {
        return NodeOperator::BranchGetRightChild(pos);
    }
};

class MutNodeOperator : public NodeOperator {
public:
    MutNodeOperator(const BTree* btree, PageId page_id) : NodeOperator{ btree, page_id, true } {}

    MutNodeOperator Copy() const;
};

} // namespace yudb