#include "noder.h"

#include "btree.h"
#include "pager.h"

namespace yudb {

Noder::Noder(const BTree* btree, PageId page_id) :
    btree_{ btree },
    page_ref_{ btree_->bucket().pager().Reference(page_id) },
    node_{ reinterpret_cast<Node*>(page_ref_.page_cache()) },
    blocker_{ this } {}

Noder::Noder(const BTree* btree, PageReferencer page_ref) :
    btree_{ btree },
    page_ref_{ std::move(page_ref) },
    node_{ reinterpret_cast<Node*>(page_ref_.page_cache()) },
    blocker_{ this } {}


Noder Noder::Copy() const {
    auto& pager = btree_->bucket().pager();
    pager.Copy(page_ref_);
    auto new_pgid = pager.Alloc(1);
    Noder new_noder{ btree_, new_pgid };
    std::memcpy(new_noder.page_cache(), page_cache(), pager.page_size());
    return new_noder;
}

void Noder::SpanClear() {
    blocker_.BlockRecordClear();
}

Span Noder::SpanAlloc(std::span<const uint8_t> data) {
    Span span;
    if (data.size() <= sizeof(Span::embed.data)) {
        span.type = Span::Type::kEmbed;
        span.embed.size = data.size();
        std::memcpy(span.embed.data, data.data(), data.size());
    }
    else if (data.size() <= btree_->bucket().pager().page_size()) {
        auto res = blocker_.BlockAlloc(data.size());
        auto [index, offset] = res;
        span.type = Span::Type::kBlock;
        span.block.record_index = index;
        span.block.offset = offset;
        span.block.size = data.size();

        auto [buf, page] = blocker_.BlockLoad(span.block.record_index, span.block.offset);
        std::memcpy(buf, data.data(), data.size());

        //printf("alloc\n"); blocker_.Print(); printf("\n");
    }
    else {
        throw std::runtime_error("unrealized types.");
    }
    return span;
}

bool Noder::SpanNeed(size_t size) {
    if (size <= sizeof(Span::embed.data)) {
        return true;
    }
    else if (size <= btree_->bucket().pager().page_size()) {
        return blocker_.BlockNeed(size);
    }
    else {
        throw std::runtime_error("unrealized types.");
    }
}

size_t Noder::SpanSize(const Span& span) {
    if (span.type == Span::Type::kEmbed) {
        return span.embed.size;
    }
    else if (span.type == Span::Type::kBlock) {
        return span.block.size;
    }
    else {
        throw std::runtime_error("unrealized types.");
    }
}


void Noder::FreeSizeInit() {
    auto max_free_size = btree_->bucket().pager().page_size() - (sizeof(Node) - sizeof(Node::body));
    node_->free_size = max_free_size - (node_->block_record_count * sizeof(BlockRecord));
    if (IsBranch()) {
        node_->free_size -= sizeof(Node::body.tail_child);
        node_->free_size -= node_->element_count * sizeof(Node::BranchElement);
    }
    else if (IsLeaf()) {
        node_->free_size -= node_->element_count * sizeof(Node::LeafElement);
    }
}

BlockRecord* Noder::BlockRecordArray() {
    auto page_size = btree_->bucket().pager().page_size();
    auto offset = page_size - node_->block_record_count * sizeof(BlockRecord);
    return reinterpret_cast<BlockRecord*>(&page_cache()[offset]);
}

void Noder::BlockRecordAlloc() {
    assert(node_->free_size >= sizeof(BlockRecord));
    node_->block_record_count++;
    node_->free_size -= sizeof(BlockRecord);
}

} // namespace yudb