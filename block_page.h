#pragma once

#include <cassert>

#include "noncopyable.h"
#include "block_format.h"
#include "page.h"

namespace yudb {

class BlockManager;

class BlockPageImpl : noncopyable {
public:
    BlockPageImpl(BlockManager* block_manager, Page page_ref, BlockTableEntry* table_entry);

    void Build();
    PageOffset Alloc(PageSize size);
    void Free(PageOffset free_pos, PageSize free_size);
    uint8_t* Load(PageOffset pos);

    Page Release() {
        table_entry_ = nullptr;
        format_ = nullptr;
        arena_.set_format(nullptr);
        return std::move(page_);
    }
    bool Copy();

    PageId page_id() {
        return page_.page_id();
    }
    void set_table_entry(BlockTableEntry* table_entry) { table_entry_ = table_entry; }
    auto& format() { return *format_; }
    auto& fragment_size() { return format_->fragment_size; }
    auto& block_table() { return format_->block_table; }
    auto& block_table() const { return format_->block_table; }
    auto content(PageOffset pos) { return &format_->page[pos]; };
    auto& arena() { return arena_; }

private:
    void UpdateMaxFreeSize();

protected:
    BlockManager* block_manager_;
    BlockTableEntry* table_entry_;
    Page page_;
    BlockPageFormat* format_;
    PageArena arena_;
};

class ConstBlockPage : BlockPageImpl {
public:
    ConstBlockPage(BlockManager* block_manager, Page page_ref, const BlockTableEntry* table_entry);
    ConstBlockPage(BlockManager* block_manager, Page page_ref, uint16_t page_index_of_block_table);
    ConstBlockPage(BlockManager* block_manager, PageId pgid, const BlockTableEntry* table_entry);
    ConstBlockPage(BlockManager* block_manager, PageId pgid, uint16_t page_index_of_block_table);

    const uint8_t* Load(PageOffset pos) {
        return BlockPageImpl::Load(pos);
    }

    ConstPage Release() {
        return BlockPageImpl::Release();
    }

    PageId page_id() {
        return BlockPageImpl::page_id();
    }
    const auto& format() { return *format_; }
    const auto& fragment_size() { return format_->fragment_size; }
    const auto& block_table() { return format_->block_table; }
    const uint8_t* content(PageOffset pos) { return &format_->page[pos]; };
    const auto& arena() { return arena_; }
};

class BlockPage : public BlockPageImpl {
public:
    BlockPage(BlockManager* block_manager, Page page_ref, BlockTableEntry* table_entry);
    BlockPage(BlockManager* block_manager, Page page_ref, uint16_t page_index_of_block_table);
    BlockPage(BlockManager* block_manager, PageId pgid, BlockTableEntry* table_entry);
    BlockPage(BlockManager* block_manager, PageId pgid, uint16_t page_index_of_block_table);
};

} // namespace yudb