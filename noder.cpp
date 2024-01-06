#include "noder.h"

#include "btree.h"
#include "pager.h"

namespace yudb {

Noder::Noder(const BTree* btree, PageId page_id) :
    btree_{ btree },
    page_ref_{ btree_->bucket().pager().Reference(page_id) },
    node_{ reinterpret_cast<Node*>(page_ref_.page_cache()) },
    overflower_{ this, &node_->overflow_info } {}

Noder::Noder(const BTree* btree, PageReferencer page_ref) :
    btree_{ btree },
    page_ref_{ std::move(page_ref) },
    node_{ reinterpret_cast<Node*>(page_ref_.page_cache()) },
    overflower_{ this, &node_->overflow_info } {}


Noder Noder::Copy() const {
    auto& pager = btree_->bucket().pager();
    pager.Copy(page_ref_);
    auto new_pgid = pager.Alloc(1);
    Noder new_noder{ btree_, new_pgid };
    std::memcpy(new_noder.page_cache(), page_cache(), pager.page_size());
    return new_noder;
}

void Noder::SpanClear() {
    overflower_.OverflowInfoClear();
}


} // namespace yudb