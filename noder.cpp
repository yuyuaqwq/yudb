#include "noder.h"

#include "btree.h"
#include "pager.h"

namespace yudb {


Noder::Noder(const BTree * btree, PageId page_id, bool dirty) :
    btree_{ btree },
    page_ref_{ btree_->bucket().pager().Reference(page_id, dirty) },
    node_{ &page_ref_.page_cache<Node>() },
    page_spacer_{ &btree_->bucket().pager(), &node_->page_space },
    blocker_{ Blocker{this, &node_->block_info } } {}

Noder::Noder(const BTree* btree, PageReferencer page_ref) :
    btree_{ btree },
    page_ref_{ std::move(page_ref) },
    node_{ &page_ref_.page_cache<Node>() },
    page_spacer_{ &btree_->bucket().pager(), &node_->page_space },
    blocker_{ Blocker{this, &node_->block_info} } {}

Noder::Noder(Noder&& right) noexcept :
    page_ref_{ std::move(right.page_ref_) },
    btree_{ right.btree_ },
    node_{ right.node_ },
    page_spacer_{ std::move(right.page_spacer_) },
    blocker_{ std::move(right.blocker_) }
{
    blocker_.set_noder(this);
}

void Noder::operator=(Noder&& right) noexcept {
    page_ref_ = std::move(right.page_ref_);
    btree_ = right.btree_;
    node_ = right.node_;
    blocker_ = std::move(right.blocker_);
    blocker_.set_noder(this);
}



void Noder::CellClear() {
    blocker_.InfoClear();
}


void Noder::PageSpaceInit() {
    auto& pager = btree_->bucket().pager();
    PageSpacer spacer{ &pager, &node_->page_space };
    spacer.Build();
    spacer.AllocLeft(sizeof(Node) - sizeof(Node::body));
}

void Noder::LeafAlloc(uint16_t ele_count) {
    LeafCheck();
    assert(ele_count * sizeof(Node::LeafElement) <= node_->page_space.rest_size);
    page_spacer_.AllocLeft(ele_count * sizeof(Node::LeafElement));
    node_->element_count += ele_count;
}

void Noder::LeafFree(uint16_t ele_count) {
    LeafCheck();
    assert(node_->element_count >= ele_count);
    page_spacer_.FreeLeft(ele_count * sizeof(Node::LeafElement));
    node_->element_count -= ele_count;
}

void Noder::BranchAlloc(uint16_t ele_count) {
    BranchCheck();
    assert(ele_count * sizeof(Node::BranchElement) <= node_->page_space.rest_size);
    page_spacer_.AllocLeft(ele_count * sizeof(Node::BranchElement));
    node_->element_count += ele_count;
}

void Noder::BranchFree(uint16_t ele_count) {
    BranchCheck();
    assert(node_->element_count >= ele_count);
    page_spacer_.FreeLeft(ele_count * sizeof(Node::BranchElement));
    node_->element_count -= ele_count;
}


void Noder::LeafCheck() {
    assert(btree_->bucket().pager().page_size() == (sizeof(Node) - sizeof(Node::body)) + node_->element_count * sizeof(Node::LeafElement) + node_->page_space.rest_size);
}

void Noder::BranchCheck() {
    assert(btree_->bucket().pager().page_size() == (sizeof(Node) - sizeof(Node::body)) + node_->element_count * sizeof(Node::BranchElement) + node_->page_space.rest_size);
}


MutNoder MutNoder::Copy() const {
    auto& pager = btree_->bucket().pager();
    pager.Copy(page_ref_);
    auto new_pgid = pager.Alloc(1);
    MutNoder new_noder{ btree_, new_pgid };
    std::memcpy(&new_noder.page_cache<uint8_t>(), &page_cache<uint8_t>(), pager.page_size());
    return new_noder;
}


} // namespace yudb