#include "node.h"

#include "bucket_impl.h"
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
    block_manager_{ std::move(right.block_manager_) } {}
void Node::operator=(Node&& right) noexcept {
    page_ref_ = std::move(right.page_ref_);
    btree_ = right.btree_;
    node_format_ = right.node_format_;
    page_arena_.set_arena_format(&node_format_->page_arena_format);
    block_manager_ = std::move(right.block_manager_);
}

std::tuple<const uint8_t*, uint32_t, std::optional<std::variant<PageReference, std::string>>>
Node::CellLoad(const Cell& cell) {
    if (cell.type == Cell::Type::kEmbed) {
        return { cell.embed.data, cell.embed.size, std::nullopt };
    }
    else if (cell.type == Cell::Type::kBlock) {
        auto [buff, page] = block_manager_.Load(cell.block.entry_index(), cell.block.offset);
        return { buff, cell.block.size, std::move(page) };
    }
    else if (cell.type == Cell::Type::kPage) {
        auto [buff, page] = block_manager_.Load(cell.block.entry_index(), cell.block.offset);
        auto page_record = reinterpret_cast<const std::pair<PageId, uint32_t>*>(buff);
        auto& pager = btree_->bucket().pager();
        auto page_size = pager.page_size();
        auto page_count = page_record->second / page_size;
        if (page_record->second % page_size) ++page_count;
        auto pgid = pager.Alloc(page_count);
        uint32_t rem_size = page_record->second;
        std::string str;
        str.resize(rem_size, 0);
        if (page_count <= kMaxCachedPageCount) {
            for (uint32_t i = 0; i < page_count; i++) {
                auto page_ref = pager.Reference(pgid + i, true);
                auto buf = &page_ref.content<uint8_t>();
                std::memcpy(&str[i * page_size], buf, std::min(static_cast<uint32_t>(page_size), rem_size));
                rem_size -= page_size;
            }
        }
        else {
            pager.Read(pgid, str.data(), page_count);
        }
        return { buff, cell.block.size, std::move(str) };
    }
    else {
        throw std::runtime_error("unrealized types.");
    }
}
size_t Node::CellSize(const Cell& cell) {
    if (cell.type == Cell::Type::kEmbed) {
        return cell.embed.size;
    }
    else if (cell.type == Cell::Type::kBlock) {
        return cell.block.size;
    }
    else if (cell.type == Cell::Type::kPage) {
        auto [buff, page] = block_manager_.Load(cell.block.entry_index(), cell.block.offset);
        auto page_record = reinterpret_cast<const std::pair<PageId, uint32_t>*>(buff);
        return page_record->second;
    }
    else {
        throw std::runtime_error("unrealized types.");
    }
}
Cell Node::CellAlloc(std::span<const uint8_t> data) {
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
        cell.block.set_entry_index(index);
        cell.block.offset = offset;
        cell.block.size = data.size();

        auto [buf, page] = block_manager_.MutLoad(cell.block.entry_index(), cell.block.offset);
        std::memcpy(buf, data.data(), data.size());
    }
    else {
        auto& pager = btree_->bucket().pager();
        auto page_size = pager.page_size();
        auto page_count = data.size() / page_size;
        if (data.size() % page_size) ++page_count;
        auto pgid = pager.Alloc(page_count);
        std::pair<PageId, uint32_t> page_record{ pgid, data.size() };
        cell =  CellAlloc({ reinterpret_cast<uint8_t*>(&page_record), sizeof(page_record)});
        uint32_t rem_size = data.size();
        if (page_count <= kMaxCachedPageCount) {
            for (uint32_t i = 0; i < page_count; i++) {
                auto page_ref = pager.Reference(pgid + i, true);
                auto buf = &page_ref.content<uint8_t>();
                std::memcpy(buf, &data[i * page_size], std::min(static_cast<uint32_t>(page_size), rem_size));
                rem_size -= page_size;
            }
        }
        else {
            pager.Write(pgid, data.data(), page_count);
        }
        cell.type = Cell::Type::kPage;
    }
    return cell;
}
void Node::CellFree(Cell&& cell) {
    if (cell.type == Cell::Type::kInvalid) {}
    else if (cell.type == Cell::Type::kEmbed) {}
    else if (cell.type == Cell::Type::kBlock) {
        block_manager_.Free({ cell.block.entry_index(), cell.block.offset, cell.block.size });
        //printf("free\n"); blocker_.Print(); printf("\n");
    }
    else if (cell.type == Cell::Type::kPage) {
        auto [buff, page] = block_manager_.Load(cell.block.entry_index(), cell.block.offset);
        auto page_record = reinterpret_cast<const std::pair<PageId, uint32_t>*>(buff);
        auto& pager = btree_->bucket().pager();
        auto page_size = pager.page_size();
        auto page_count = page_record->second / page_size;
        if (page_record->second % page_size) ++page_count;
        pager.Free(page_record->first, page_count);
        CellFree(std::move(cell));
    }
    else {
        throw std::runtime_error("unrealized types.");
    }
    cell.type = Cell::Type::kInvalid;
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


void Node::BlockRealloc(BlockPage* new_page, uint16_t entry_index, Cell* cell) {
    if (cell->type != Cell::Type::kBlock) {
        return;
    }
    if (cell->block.entry_index() != entry_index) {
        return;
    }
    cell->block.offset = block_manager_.PageRebuildAppend(new_page, { entry_index, cell->block.offset, cell->block.size });
}
void Node::DefragmentSpace(uint16_t index) {
    auto new_page = block_manager_.PageRebuildBegin(index);
    for (uint16_t i = 0; i < node_format_->element_count; i++) {
        if (IsBranch()) {
            BlockRealloc(&new_page, index, &node_format_->body.branch[i].key);
        } else {
            assert(IsLeaf());
            BlockRealloc(&new_page, index, &node_format_->body.leaf[i].key);
            BlockRealloc(&new_page, index, &node_format_->body.leaf[i].value);
        }
    }
    block_manager_.PageRebuildEnd(&new_page, index);
}


MutNode MutNode::Copy() const {
    auto& pager = btree_->bucket().pager();
    pager.Copy(page_ref_);
    auto new_pgid = pager.Alloc(1);
    MutNode new_node{ btree_, new_pgid, false };
    std::memcpy(&new_node.page_content<uint8_t>(), &page_content<uint8_t>(), pager.page_size());
    return new_node;
}

} // namespace yudb