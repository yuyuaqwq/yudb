#include "block_manager.h"

#include <format>

#include "node_operator.h"
#include "pager.h"

#include "btree.h"
#include "tx.h"

namespace yudb {

PageSize BlockManager::MaxSize() {
    auto& pager = node_operator_->btree().bucket().pager();
    auto size = pager.page_size() - (sizeof(BlockPage) - sizeof(BlockPage::block_table));
    return size;
}

std::pair<uint8_t*, PageReference> BlockManager::Load(uint16_t index, PageOffset pos) {
    auto& pager = node_operator_->btree().bucket().pager();

    auto page_ref = pager.Reference(block_table_descriptor_->pgid, false);
    auto& block_page = page_ref.page_content<BlockPage>();
    auto data_page = pager.Reference(block_page.block_table[index].pgid, false);
    auto& data_cache = data_page.page_content<BlockPage>();
    return { &data_cache.page[pos], std::move(data_page) };
}

std::optional<std::pair<uint16_t, PageOffset>> BlockManager::Alloc(PageSize size) {
    auto& pager = node_operator_->btree().bucket().pager();

    assert(size <= MaxSize());

    if (block_table_descriptor_->pgid == kPageInvalidId) {
        Build();
    }
    TablePageCopy();

    auto table_page_ref = pager.Reference(block_table_descriptor_->pgid, true);
    auto& table_block_page = table_page_ref.page_content<BlockPage>();
    auto block_table = table_block_page.block_table;
    for (uint16_t i = 0; i < block_table_descriptor_->count; i++) {
        auto& entry = block_table[i];
        if (entry.pgid == table_page_ref.page_id()) {
            // 是reocrd_arr所在的页面，如果空间不足则尝试分配
            if (entry.max_free_size < size) {
                PageSpaceOperator space_oper{ &pager, &table_block_page.page_space };
                auto res = space_oper.AllocRight(size);
                if (res) {
                    Free({ i, *res, size});
                }
            }
        }
        if (entry.max_free_size >= size) {
            PageCopy(&entry);

            auto page_ref = pager.Reference(entry.pgid, true);
            auto& block_page = page_ref.page_content<BlockPage>();
            // 找到足够分配的FreeBlock
            auto prev_pos = kPageInvalidOffset;
            auto cur_pos = block_page.first_block_pos;
            while (cur_pos != kPageInvalidOffset) {
                auto free_block = reinterpret_cast<FreeBlock*>(&block_page.page[cur_pos]);
                if (free_block->size >= size) {
                    break;
                }
                assert(free_block->next != kPageInvalidOffset);
                prev_pos = cur_pos;
                cur_pos = free_block->next;
            }
            assert(cur_pos != kPageInvalidOffset);

            auto free_block = reinterpret_cast<FreeBlock*>(&block_page.page[cur_pos]);
            auto new_pos = kPageInvalidOffset;
            auto new_size = free_block->size - size;
            if (new_size == 0) {
                new_pos = free_block->next;
            }
            else {
                if (new_size < sizeof(FreeBlock)) {
                    block_page.fragment_size += new_size;
                    new_pos = free_block->next;
                }
                else {
                    new_pos = cur_pos + size;
                    auto new_free_block = reinterpret_cast<FreeBlock*>(&block_page.page[new_pos]);
                    new_free_block->next = free_block->next;
                    new_free_block->size = new_size;
                }
            }

            if (block_page.first_block_pos == cur_pos) {
                block_page.first_block_pos = new_pos;
            }
            else {
                auto prev_free_block = reinterpret_cast<FreeBlock*>(&block_page.page[prev_pos]);
                prev_free_block->next = new_pos;
            }

            if (free_block->size == entry.max_free_size) {
                TableEntryUpdateMaxFreeSize(&entry, &block_page);
            }
            return std::pair{ i, cur_pos };
        }
    }

    // 没有足够分配的空间，分配新页
    PageAppend(&table_page_ref);
    return Alloc(size);
}

void BlockManager::Free(const std::tuple<uint16_t, PageOffset, PageSize>& free_block) {
    TablePageCopy();

    auto& pager = node_operator_->btree().bucket().pager();

    auto [index, free_pos, free_size] = free_block;

    auto table_page_ref = pager.Reference(block_table_descriptor_->pgid, true);
    auto& table_block_page = table_page_ref.page_content<BlockPage>();
    auto& entry = table_block_page.block_table[index];

    PageCopy(&entry);
    auto page_ref = pager.Reference(entry.pgid, true);
    auto& block_page = page_ref.page_content<BlockPage>();

    auto cur_pos = block_page.first_block_pos;
    auto prev_pos = kPageInvalidOffset;
    auto next_pos = kPageInvalidOffset;

    // 查找是否存在可合并的块
    auto cur_prev_pos = kPageInvalidOffset;
    while (cur_pos != kPageInvalidOffset) {
        auto del = false;
        auto cur_free_block = reinterpret_cast<FreeBlock*>(&block_page.page[cur_pos]);
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
            if (block_page.first_block_pos == cur_pos) {
                block_page.first_block_pos = cur_free_block->next;
            }
            else {
                auto cur_prev_free_block = reinterpret_cast<FreeBlock*>(&block_page.page[cur_prev_pos]);
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
        auto prev_free_block = reinterpret_cast<FreeBlock*>(&block_page.page[prev_pos]);
        free_size += prev_free_block->size;
        free_pos = prev_pos;
    }
    if (next_pos != kPageInvalidOffset) {
        auto next_free_block = reinterpret_cast<FreeBlock*>(&block_page.page[next_pos]);
        free_size += next_free_block->size;
    }

    assert(free_size >= sizeof(FreeBlock));
    auto free_free_block = reinterpret_cast<FreeBlock*>(&block_page.page[free_pos]);
    free_free_block->next = block_page.first_block_pos;
    free_free_block->size = free_size;

    block_page.first_block_pos = free_pos;
    if (free_size > entry.max_free_size) {
        entry.max_free_size = free_size;
    }
}

void BlockManager::Print() {
    auto& pager = node_operator_->btree().bucket().pager();
    auto page_ref = pager.Reference(block_table_descriptor_->pgid, false);
    auto& block_page = page_ref.page_content<BlockPage>();
    auto block_table = block_page.block_table;
    for (uint16_t i = 0; i < block_table_descriptor_->count; i++) {
        auto alloc_page = pager.Reference(block_table[i].pgid, false);
        auto& alloc_cache = alloc_page.page_content<BlockPage>();

        auto j = 0;
        auto prev_pos = kPageInvalidOffset;
        auto cur_pos = alloc_cache.first_block_pos;
        while (cur_pos != kPageInvalidOffset) {
            auto free_block = reinterpret_cast<FreeBlock*>(&alloc_cache.page[cur_pos]);
            std::cout << std::format("[FreeBlock][pgid:{}][pos:{}]:{}", block_table[i].pgid, cur_pos, free_block->size) << std::endl;
            prev_pos = cur_pos;
            cur_pos = free_block->next;
            ++j;
        }
        if (j == 0) {
            std::cout << std::format("[FreeBlock][pgid:{}]:null", block_table[i].pgid) << std::endl;
        }
    }
}


void BlockManager::Build() {
    auto& pager = node_operator_->btree().bucket().pager();

    block_table_descriptor_->pgid = pager.Alloc(1);
    block_table_descriptor_->entry_index = 0;
    block_table_descriptor_->count = 1;

    auto page_ref = pager.Reference(block_table_descriptor_->pgid, true);
    auto& block_page = PageBuild(&page_ref);

    TableEntryBuild(&block_page.block_table[0], 0, &page_ref);
}

void BlockManager::Clear() {
    if (block_table_descriptor_->pgid == kPageInvalidId) {
        return;
    }
    TablePageCopy();

    auto& pager = node_operator_->btree().bucket().pager();
    auto page_ref = pager.Reference(block_table_descriptor_->pgid, true);
    auto& block_page = page_ref.page_content<BlockPage>();
    for (uint16_t i = 0; i < block_table_descriptor_->count; i++) {
        pager.Free(block_page.block_table[i].pgid, 1);
    }
    block_table_descriptor_->pgid = kPageInvalidId;
}


void BlockManager::TableEntryBuild(BlockTableEntry* entry, uint16_t index, PageReference* new_page_ref) {
    auto& pager = node_operator_->btree().bucket().pager();
    entry->pgid = new_page_ref->page_id();
    entry->max_free_size = 0;
    auto& block_page = new_page_ref->page_content<BlockPage>();
    PageSpaceOperator space_oper{ &pager, &block_page.page_space };
    if (entry->pgid == node_operator_->node().block_info.pgid) {
        space_oper.AllocLeft(sizeof(BlockTableEntry));
        return;
    }
    auto rest_size = space_oper.rest_size();
    auto pos = space_oper.AllocLeft(rest_size);
    assert(pos.has_value());
    Free({ index, *pos, rest_size });
}

void BlockManager::TableEntryUpdateMaxFreeSize(BlockTableEntry* entry, BlockPage* block_page) {
    auto max_free_size = 0;
    auto cur_pos = block_page->first_block_pos;
    while (cur_pos != kPageInvalidOffset) {
        auto free_block = reinterpret_cast<FreeBlock*>(&block_page->page[cur_pos]);
        if (free_block->size > max_free_size) {
            max_free_size = free_block->size;
        }
        cur_pos = free_block->next;
    }
    entry->max_free_size = max_free_size;
}

void BlockManager::TablePageCopy() {
    auto& bucket = node_operator_->btree().bucket();
    auto& pager = bucket.pager();
    auto& tx = bucket.tx();

    auto page = pager.Reference(block_table_descriptor_->pgid, false);
    auto& block_page = page.page_content<BlockPage>();
    if (tx.NeedCopy(block_page.last_modified_txid)) {
        auto new_page_ref = pager.Copy(std::move(page));
        auto& new_block_page = page.page_content<BlockPage>();
        new_block_page.last_modified_txid = tx.txid();
        block_table_descriptor_->pgid = page.page_id();
        new_block_page.block_table[block_table_descriptor_->entry_index].pgid = page.page_id();
    }
}


void BlockManager::PageAppend(PageReference* table_page_ref) {
    assert(sizeof(BlockTableEntry) * (block_table_descriptor_->count + 1) <= MaxSize());

    auto& pager = node_operator_->btree().bucket().pager();

    auto new_pgid = pager.Alloc(1);
    auto new_page = pager.Reference(new_pgid, true);
    auto& new_block_page = PageBuild(&new_page);
    
    auto* table_block_page = &table_page_ref->page_content<BlockPage>();
    PageSpaceOperator space_oper{ &pager, &table_block_page->page_space };
    BlockTableEntry* block_table = table_block_page->block_table;
    if (!space_oper.AllocLeft(sizeof(BlockTableEntry))) {
        // 分配失败的话，将位于Left区间的Table移动到新页中
        BlockTableEntry* new_block_table = new_block_page.block_table;
        auto byte_size = sizeof(BlockTableEntry) * block_table_descriptor_->count;
        std::memcpy(new_block_table, block_table, byte_size);

        // 释放Left区间，分配到Right区间
        space_oper.FreeLeft(byte_size);
        auto rest_size = space_oper.rest_size();
        auto res = space_oper.AllocRight(rest_size);
        assert(res.has_value());

        // Free使用新的页面
        auto old_entry_index = block_table_descriptor_->entry_index;
        block_table_descriptor_->pgid = new_pgid;
        block_table_descriptor_->entry_index = block_table_descriptor_->count;
        Free({ old_entry_index, *res, rest_size });

        PageSpaceOperator space_oper{ &pager, &new_block_page.page_space };
        space_oper.AllocLeft(sizeof(BlockTableEntry) * block_table_descriptor_->count);
        TableEntryBuild(&new_block_table[block_table_descriptor_->count], block_table_descriptor_->count , &new_page);
    }
    else {
        TableEntryBuild(&block_table[block_table_descriptor_->count], block_table_descriptor_->count, &new_page);
    }
    ++block_table_descriptor_->count;
}

void BlockManager::PageCopy(BlockTableEntry* entry) {
    auto& bucket = node_operator_->btree().bucket();
    auto& pager = bucket.pager();
    auto& tx = bucket.tx();

    auto page_ref = pager.Reference(entry->pgid, false);
    auto& block_page = page_ref.page_content<BlockPage>();
    if (tx.NeedCopy(block_page.last_modified_txid)) {
        auto new_page = pager.Copy(std::move(page_ref));
        entry->pgid = new_page.page_id();
    }
}

BlockPage& BlockManager::PageBuild(PageReference* page_ref) {
    auto& pager = node_operator_->btree().bucket().pager();
    auto& block_page = page_ref->page_content<BlockPage>();

    block_page.last_modified_txid = node_operator_->btree().bucket().tx().txid();
    block_page.fragment_size = 0;
    block_page.first_block_pos = kPageInvalidOffset;

    PageSpaceOperator space_oper{ &pager, &block_page.page_space };
    space_oper.Build();
    space_oper.AllocLeft(sizeof(BlockPage) - sizeof(BlockPage::block_table));

    return block_page;
}


} // namespace yudb