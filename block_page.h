#pragma once

#include <cassert>

#include "noncopyable.h"
#include "block_format.h"
#include "page_reference.h"

namespace yudb {

class BlockManager;

class BlockPage : noncopyable {
public:
    BlockPage(BlockManager* block_manager, PageReference page_ref, BlockTableEntry* table_entry);
    BlockPage(BlockManager* block_manager, PageId pgid, BlockTableEntry* table_entry);
    BlockPage(BlockManager* block_manager, PageReference page_ref, uint16_t page_index_of_block_table);
    BlockPage(BlockManager* block_manager, PageId pgid, uint16_t page_index_of_block_table);
    
    void set_table_entry(BlockTableEntry* table_entry) { table_entry_ = table_entry; }
    auto& format() { return *format_; }
    PageSize fragment_size() { return format_->fragment_size; }
    BlockTableEntry* block_table() { return format_->block_table; }
    uint8_t* content(PageOffset pos) { return &format_->page[pos]; };
    PageArena& arena() { return arena_; }

    void Build();
    PageOffset Alloc(PageSize size);
    void Free(PageOffset free_pos, PageSize free_size);
    uint8_t* Load(PageOffset pos);
    bool Copy();

    PageReference ReleasePageReference() {
        format_ = nullptr;
        table_entry_ = nullptr;
        return std::move(page_ref_);
    }

    PageId page_id() {
        return page_ref_.page_id();
    }

private:
    void UpdateMaxFreeSize();

private:
    BlockManager* block_manager_;
    BlockTableEntry* table_entry_;
    PageReference page_ref_;
    BlockPageFormat* format_;
    PageArena arena_;
};

} // namespace yudb