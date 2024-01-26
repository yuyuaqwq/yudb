#include "node_operator.h"

#include "btree.h"
#include "pager.h"

namespace yudb {


NodeOperator::NodeOperator(const BTree * btree, PageId page_id, bool dirty) :
    btree_{ btree },
    page_ref_{ btree_->bucket().pager().Reference(page_id, dirty) },
    node_{ &page_ref_.page_content<Node>() },
    page_spacer_{ &btree_->bucket().pager(), &node_->page_space },
    blocker_{ BlockManager{this, &node_->block_info } } {}

NodeOperator::NodeOperator(const BTree* btree, PageReference page_ref) :
    btree_{ btree },
    page_ref_{ std::move(page_ref) },
    node_{ &page_ref_.page_content<Node>() },
    page_spacer_{ &btree_->bucket().pager(), &node_->page_space },
    blocker_{ BlockManager{this, &node_->block_info} } {}

NodeOperator::NodeOperator(NodeOperator&& right) noexcept :
    page_ref_{ std::move(right.page_ref_) },
    btree_{ right.btree_ },
    node_{ right.node_ },
    page_spacer_{ std::move(right.page_spacer_) },
    blocker_{ std::move(right.blocker_) }
{
    blocker_.set_node_operator(this);
}

void NodeOperator::operator=(NodeOperator&& right) noexcept {
    page_ref_ = std::move(right.page_ref_);
    btree_ = right.btree_;
    node_ = right.node_;
    blocker_ = std::move(right.blocker_);
    blocker_.set_node_operator(this);
}



void NodeOperator::CellClear() {
    blocker_.Clear();
}


void NodeOperator::PageSpaceInit() {
    auto& pager = btree_->bucket().pager();
    PageSpaceOperator spacer{ &pager, &node_->page_space };
    spacer.Build();
    spacer.AllocLeft(sizeof(Node) - sizeof(Node::body));
}

void NodeOperator::LeafAlloc(uint16_t ele_count) {
    LeafCheck();
    assert(ele_count * sizeof(Node::LeafElement) <= node_->page_space.rest_size);
    page_spacer_.AllocLeft(ele_count * sizeof(Node::LeafElement));
    node_->element_count += ele_count;
}

void NodeOperator::LeafFree(uint16_t ele_count) {
    LeafCheck();
    assert(node_->element_count >= ele_count);
    page_spacer_.FreeLeft(ele_count * sizeof(Node::LeafElement));
    node_->element_count -= ele_count;
}

void NodeOperator::BranchAlloc(uint16_t ele_count) {
    BranchCheck();
    assert(ele_count * sizeof(Node::BranchElement) <= node_->page_space.rest_size);
    page_spacer_.AllocLeft(ele_count * sizeof(Node::BranchElement));
    node_->element_count += ele_count;
}

void NodeOperator::BranchFree(uint16_t ele_count) {
    BranchCheck();
    assert(node_->element_count >= ele_count);
    page_spacer_.FreeLeft(ele_count * sizeof(Node::BranchElement));
    node_->element_count -= ele_count;
}


void NodeOperator::LeafCheck() {
    assert(btree_->bucket().pager().page_size() == (sizeof(Node) - sizeof(Node::body)) + node_->element_count * sizeof(Node::LeafElement) + node_->page_space.rest_size);
}

void NodeOperator::BranchCheck() {
    assert(btree_->bucket().pager().page_size() == (sizeof(Node) - sizeof(Node::body)) + node_->element_count * sizeof(Node::BranchElement) + node_->page_space.rest_size);
}


MutNodeOperator MutNodeOperator::Copy() const {
    auto& pager = btree_->bucket().pager();
    pager.Copy(page_ref_);
    auto new_pgid = pager.Alloc(1);
    MutNodeOperator new_node{ btree_, new_pgid };
    std::memcpy(&new_node.page_content<uint8_t>(), &page_content<uint8_t>(), pager.page_size());
    return new_node;
}


} // namespace yudb