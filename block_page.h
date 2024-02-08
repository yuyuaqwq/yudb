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
    
    void set_table_entry(BlockTableEntry* table_entry) { table_entry_ = table_entry; }
    auto& format() { return *format_; }
    auto& fragment_size() { return format_->fragment_size; }
    auto& block_table() { return format_->block_table; }
    auto content(PageOffset pos) { return &format_->page[pos]; };
    auto& arena() { return arena_; }

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

protected:
    BlockManager* block_manager_;
    BlockTableEntry* table_entry_;
    PageReference page_ref_;
    BlockPageFormat* format_;
    PageArena arena_;
};

class ImmBlockPage : BlockPage {
public:
    ImmBlockPage(BlockManager* block_manager, PageReference page_ref, const BlockTableEntry* table_entry);
    ImmBlockPage(BlockManager* block_manager, PageReference page_ref, uint16_t page_index_of_block_table);
    ImmBlockPage(BlockManager* block_manager, PageId pgid, const BlockTableEntry* table_entry);
    ImmBlockPage(BlockManager* block_manager, PageId pgid, uint16_t page_index_of_block_table);

    uint8_t* Load(PageOffset pos) {
        return BlockPage::Load(pos);
    }


    PageReference ReleasePageReference() {
        return BlockPage::ReleasePageReference();
    }

    PageId page_id() {
        return BlockPage::page_id();
    }

    const auto& format() { return *format_; }
    const auto& fragment_size() { return format_->fragment_size; }
    const auto& block_table() { return format_->block_table; }
    const uint8_t* content(PageOffset pos) { return &format_->page[pos]; };
    const auto& arena() { return arena_; }

};

class MutBlockPage : public BlockPage {
public:
    MutBlockPage(BlockManager* block_manager, PageReference page_ref, BlockTableEntry* table_entry);
    MutBlockPage(BlockManager* block_manager, PageReference page_ref, uint16_t page_index_of_block_table);
    MutBlockPage(BlockManager* block_manager, PageId pgid, BlockTableEntry* table_entry);
    MutBlockPage(BlockManager* block_manager, PageId pgid, uint16_t page_index_of_block_table);
};

} // namespace yudb