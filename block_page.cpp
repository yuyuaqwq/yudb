#include "block_page.h"

#include "pager.h"
#include "block_manager.h"
#include "node.h"
#include "bucket.h"
#include "tx.h"

namespace yudb {

BlockPageImpl::BlockPageImpl(BlockManager* block_manager, Page page_ref, BlockTableEntry* table_entry) :
    block_manager_{ std::move(block_manager) },
    page_{ std::move(page_ref) },
    table_entry_{ table_entry },
    format_{ &page_.content<BlockPageFormat>() },
    arena_ { &block_manager_->node().btree().bucket().pager(), &format_->arena_format } {}

void BlockPageImpl::Build() {
    auto& node = block_manager_->node();

    format_->last_modified_txid = node.btree().bucket().tx().txid();
    format_->fragment_size = 0;
    format_->first_block_pos = kPageInvalidOffset;

    arena_.Build();
    arena_.AllocLeft(sizeof(BlockPageFormat) - sizeof(BlockPageFormat::block_table));
}


PageOffset BlockPageImpl::Alloc(PageSize size) {
    auto& pager = block_manager_->node().btree().bucket().pager();
    
    // 找到足够分配的FreeBlock
    Copy();
    auto prev_pos = kPageInvalidOffset;
    auto cur_pos = format_->first_block_pos;
    while (cur_pos != kPageInvalidOffset) {
        assert(cur_pos < pager.page_size());
        auto free_block = reinterpret_cast<FreeBlock*>(&format_->page[cur_pos]);
        if (free_block->size >= size) {
            break;
        }
        prev_pos = cur_pos;
        cur_pos = free_block->next;
    }
    assert(cur_pos != kPageInvalidOffset);

    auto free_block = reinterpret_cast<FreeBlock*>(&format_->page[cur_pos]);
    auto new_pos = kPageInvalidOffset;
    auto new_size = free_block->size - size;
    if (new_size == 0) {
        new_pos = free_block->next;
    }
    else {
        if (new_size < sizeof(FreeBlock)) {
            format_->fragment_size += new_size;
            new_pos = free_block->next;
        }
        else {
            new_pos = cur_pos + size;
            auto new_free_block = reinterpret_cast<FreeBlock*>(&format_->page[new_pos]);
            new_free_block->next = free_block->next;
            new_free_block->size = new_size;
        }
    }

    if (format_->first_block_pos == cur_pos) {
        format_->first_block_pos = new_pos;
    }
    else {
        auto prev_free_block = reinterpret_cast<FreeBlock*>(&format_->page[prev_pos]);
        prev_free_block->next = new_pos;
    }

    if (table_entry_->max_free_size == free_block->size) {
        UpdateMaxFreeSize();
    }
    return cur_pos;
}
void BlockPageImpl::Free(PageOffset free_pos, PageSize free_size) {
    auto& pager = block_manager_->node().btree().bucket().pager();

    Copy();
    auto cur_pos = format_->first_block_pos;
    auto prev_pos = kPageInvalidOffset;
    auto next_pos = kPageInvalidOffset;

    // 查找是否存在可合并的块
    auto cur_prev_pos = kPageInvalidOffset;
    while (cur_pos != kPageInvalidOffset) {
        assert(cur_pos < pager.page_size());
        auto del = false;
        auto cur_free_block = reinterpret_cast<FreeBlock*>(&format_->page[cur_pos]);
        if (cur_pos + cur_free_block->size == free_pos) {
            assert(prev_pos == kPageInvalidOffset);
            prev_pos = cur_pos;
            del = true;
        }
        else if (cur_pos == free_pos + free_size) {
            assert(next_pos == kPageInvalidOffset);
            next_pos = cur_pos;
            del = true;
        }
        if (del) {
            // 先从链表中摘下
            if (format_->first_block_pos == cur_pos) {
                format_->first_block_pos = cur_free_block->next;
            }
            else {
                auto cur_prev_free_block = reinterpret_cast<FreeBlock*>(&format_->page[cur_prev_pos]);
                cur_prev_free_block->next = cur_free_block->next;
            }
            cur_pos = cur_prev_pos;     // 保持cur_prev_pos不变
        }
        if (prev_pos != kPageInvalidOffset && next_pos != kPageInvalidOffset) break;
        cur_prev_pos = cur_pos;
        cur_pos = cur_free_block->next;
    }

    // 合并
    if (prev_pos != kPageInvalidOffset) {
        auto prev_free_block = reinterpret_cast<FreeBlock*>(&format_->page[prev_pos]);
        free_size += prev_free_block->size;
        free_pos = prev_pos;
    }
    if (next_pos != kPageInvalidOffset) {
        auto next_free_block = reinterpret_cast<FreeBlock*>(&format_->page[next_pos]);
        free_size += next_free_block->size;
    }

    if (free_size < sizeof(FreeBlock)) {
        format_->fragment_size += free_size;
        return;
    }
    auto free_free_block = reinterpret_cast<FreeBlock*>(&format_->page[free_pos]);
    free_free_block->next = format_->first_block_pos;
    free_free_block->size = free_size;

    format_->first_block_pos = free_pos;

    if (free_size > table_entry_->max_free_size) {
        table_entry_->max_free_size = free_size;
    }
}
uint8_t* BlockPageImpl::Load(PageOffset pos) {
    return &format_->page[pos];
}

/*
* 仅拷贝非Table所在页的BlockPage
*/
bool BlockPageImpl::Copy() {
    auto& pager = block_manager_->node().btree().bucket().pager();
    auto& tx = block_manager_->node().btree().bucket().tx();
    if (tx.IsLegacyTx(format_->last_modified_txid)) {
        page_ = pager.Copy(std::move(page_));
        format_ = &page_.content<BlockPageFormat>();
        format_->last_modified_txid = tx.txid();
        table_entry_->pgid = page_.page_id();
        return true;
    }
    return false;
}

void BlockPageImpl::UpdateMaxFreeSize() {
    auto& pager = block_manager_->node().btree().bucket().pager();
    
    auto max_free_size = 0;
    auto cur_pos = format_->first_block_pos;
    while (cur_pos != kPageInvalidOffset) {
        assert(cur_pos < pager.page_size());
        auto free_block = reinterpret_cast<FreeBlock*>(&format_->page[cur_pos]);
        if (free_block->size > max_free_size) {
            max_free_size = free_block->size;
        }
        cur_pos = free_block->next;
    }
    table_entry_->max_free_size = max_free_size;
}



ConstBlockPage::ConstBlockPage(BlockManager* block_manager, Page page_ref, const BlockTableEntry* table_entry) :
    BlockPageImpl{ block_manager, std::move(page_ref), const_cast<BlockTableEntry*>(table_entry) } {}
ConstBlockPage::ConstBlockPage(BlockManager* block_manager, Page page_ref, uint16_t page_index_of_block_table) :
    BlockPageImpl{ block_manager, std::move(page_ref), nullptr }
{
    table_entry_ = &format_->block_table[page_index_of_block_table];
}
ConstBlockPage::ConstBlockPage(BlockManager* block_manager, PageId pgid, const BlockTableEntry* table_entry) :
    ConstBlockPage{ block_manager, block_manager->node().btree().bucket().pager().Reference(pgid, false), table_entry } {}
ConstBlockPage::ConstBlockPage(BlockManager* block_manager, PageId pgid, uint16_t page_index_of_block_table) :
    ConstBlockPage{ block_manager, pgid, nullptr }
{
    table_entry_ = &format_->block_table[page_index_of_block_table];
}

BlockPage::BlockPage(BlockManager* block_manager, Page page_ref, BlockTableEntry* table_entry) :
    BlockPageImpl{ block_manager, std::move(page_ref), table_entry } {}
BlockPage::BlockPage(BlockManager* block_manager, Page page_ref, uint16_t page_index_of_block_table) :
    BlockPageImpl{ block_manager, std::move(page_ref), nullptr }
{
    table_entry_ = &format_->block_table[page_index_of_block_table];
}
BlockPage::BlockPage(BlockManager* block_manager, PageId pgid, BlockTableEntry* table_entry) :
    BlockPage{ block_manager, block_manager->node().btree().bucket().pager().Reference(pgid, true), table_entry } {}
BlockPage::BlockPage(BlockManager* block_manager, PageId pgid, uint16_t page_index_of_block_table) :
    BlockPage{ block_manager, pgid, nullptr }
{
    table_entry_ = &format_->block_table[page_index_of_block_table];
}

} // namespace yudb