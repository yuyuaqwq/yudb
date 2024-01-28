#include "node.h"

#include "btree.h"
#include "pager.h"

namespace yudb {

Node::Node(const BTree * btree, PageId page_id, bool dirty) :
    btree_{ btree },
    page_ref_{ btree_->bucket().pager().Reference(page_id, dirty) },
    node_format_{ &page_ref_.content<NodeFormat>() },
    page_arena_{ &btree_->bucket().pager(), &node_format_->page_arena_format },
    block_manager_{ BlockManager{this, &node_format_->block_table_descriptor } } {}

Node::Node(const BTree* btree, PageReference page_ref) :
    btree_{ btree },
    page_ref_{ std::move(page_ref) },
    node_format_{ &page_ref_.content<NodeFormat>() },
    page_arena_{ &btree_->bucket().pager(), &node_format_->page_arena_format },
    block_manager_{ BlockManager{this, &node_format_->block_table_descriptor} } {}

Node::Node(Node&& right) noexcept :
    page_ref_{ std::move(right.page_ref_) },
    btree_{ right.btree_ },
    node_format_{ right.node_format_ },
    page_arena_{ &btree_->bucket().pager(), &node_format_->page_arena_format },
    block_manager_{ std::move(right.block_manager_) }
{
    block_manager_.set_node(this);
}

void Node::operator=(Node&& right) noexcept {
    page_ref_ = std::move(right.page_ref_);
    btree_ = right.btree_;
    node_format_ = right.node_format_;
    page_arena_.set_arena_format(&node_format_->page_arena_format);
    block_manager_ = std::move(right.block_manager_);
    block_manager_.set_node(this);
}



void Node::CellClear() {
    block_manager_.Clear();
}


void Node::PageSpaceBuild() {
    auto& pager = btree_->bucket().pager();
    PageArena arena{ &pager, &node_format_->page_arena_format };
    arena.Build();
    arena.AllocLeft(sizeof(NodeFormat) - sizeof(NodeFormat::body));
}

void Node::LeafAlloc(uint16_t ele_count) {
    LeafCheck();
    assert(ele_count * sizeof(NodeFormat::LeafElement) <= node_format_->page_arena_format.rest_size);
    page_arena_.AllocLeft(ele_count * sizeof(NodeFormat::LeafElement));
    node_format_->element_count += ele_count;
}

void Node::LeafFree(uint16_t ele_count) {
    LeafCheck();
    assert(node_format_->element_count >= ele_count);
    page_arena_.FreeLeft(ele_count * sizeof(NodeFormat::LeafElement));
    node_format_->element_count -= ele_count;
}

void Node::BranchAlloc(uint16_t ele_count) {
    BranchCheck();
    assert(ele_count * sizeof(NodeFormat::BranchElement) <= node_format_->page_arena_format.rest_size);
    page_arena_.AllocLeft(ele_count * sizeof(NodeFormat::BranchElement));
    node_format_->element_count += ele_count;
}

void Node::BranchFree(uint16_t ele_count) {
    BranchCheck();
    assert(node_format_->element_count >= ele_count);
    page_arena_.FreeLeft(ele_count * sizeof(NodeFormat::BranchElement));
    node_format_->element_count -= ele_count;
}


void Node::LeafCheck() {
    assert(btree_->bucket().pager().page_size() == (sizeof(NodeFormat) - sizeof(NodeFormat::body)) + node_format_->element_count * sizeof(NodeFormat::LeafElement) + node_format_->page_arena_format.rest_size);
}

void Node::BranchCheck() {
    assert(btree_->bucket().pager().page_size() == (sizeof(NodeFormat) - sizeof(NodeFormat::body)) + node_format_->element_count * sizeof(NodeFormat::BranchElement) + node_format_->page_arena_format.rest_size);
}


MutNode MutNode::Copy() const {
    auto& pager = btree_->bucket().pager();
    pager.Copy(page_ref_);
    auto new_pgid = pager.Alloc(1);
    MutNode new_node{ btree_, new_pgid };
    std::memcpy(&new_node.page_content<uint8_t>(), &page_content<uint8_t>(), pager.page_size());
    return new_node;
}


} // namespace yudb