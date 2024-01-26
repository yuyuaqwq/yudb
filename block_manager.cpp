#include "block_manager.h"

#include <format>

#include "node_operator.h"
#include "pager.h"

#include "btree.h"
#include "tx.h"

namespace yudb {

PageSize BlockManager::MaxSize() {
    auto& pager = node_operator_->btree().bucket().pager();
    auto size = pager.page_size() - (sizeof(BlockPage) - sizeof(BlockPage::record_arr));
    return size;
}

std::pair<uint8_t*, PageReference> BlockManager::Load(uint16_t record_index, PageOffset offset) {
    auto& pager = node_operator_->btree().bucket().pager();

    auto page_ref = pager.Reference(block_info_->pgid, false);
    auto& block_page = page_ref.page_content<BlockPage>();

    auto record_arr = block_page.record_arr;
    auto data_page = pager.Reference(record_arr[record_index].pgid, false);
    auto& data_cache = data_page.page_content<BlockPage>();
    return { &data_cache.page[offset], std::move(data_page) };
}

std::optional<std::pair<uint16_t, PageOffset>> BlockManager::Alloc(PageSize size) {
    auto& pager = node_operator_->btree().bucket().pager();

    assert(size <= MaxSize());

    if (block_info_->pgid == kPageInvalidId) {
        Build();
    }
    RecordPageCopy();

    auto record_page_ref = pager.Reference(block_info_->pgid, true);
    auto& record_block_page = record_page_ref.page_content<BlockPage>();
    auto record_arr = record_block_page.record_arr;
    for (uint16_t i = 0; i < block_info_->count; i++) {
        auto& record_element = record_arr[i];
        if (record_element.pgid == record_page_ref.page_id()) {
            // 是reocrd_arr所在的页面，如果空间不足则尝试分配
            if (record_element.max_free_size < size) {
                PageSpaceOperator spacer{ &pager, &record_block_page.page_space };
                auto res = spacer.AllocRight(size);
                if (res) {
                    Free({ i, *res, size});
                }
            }
        }
        if (record_element.max_free_size >= size) {
            PageCopy(&record_element);

            auto page_ref = pager.Reference(record_element.pgid, true);
            auto& block_page = page_ref.page_content<BlockPage>();
            // 找到足够分配的FreeBlock
            auto prev_pos = kPageInvalidOffset;
            auto cur_pos = block_page.first;
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

            if (block_page.first == cur_pos) {
                block_page.first = new_pos;
            }
            else {
                auto prev_free_block = reinterpret_cast<FreeBlock*>(&block_page.page[prev_pos]);
                prev_free_block->next = new_pos;
            }

            if (free_block->size == record_element.max_free_size) {
                RecordUpdateMaxFreeSize(&record_element, &block_page);
            }
            return std::pair{ i, cur_pos };
        }
    }

    // 没有足够分配的空间，分配新页
    PageAppend(&record_page_ref);
    return Alloc(size);
}

void BlockManager::Free(const std::tuple<uint16_t, PageOffset, PageSize>& free_block) {
    RecordPageCopy();

    auto& pager = node_operator_->btree().bucket().pager();

    auto [record_index, free_pos, free_size] = free_block;

    auto record_page = pager.Reference(block_info_->pgid, true);
    auto& record_cache = record_page.page_content<BlockPage>();
    auto& record_element = record_cache.record_arr[record_index];


    PageCopy(&record_element);
    auto block_page = pager.Reference(record_element.pgid, true);
    auto& block_cache = block_page.page_content<BlockPage>();

    auto cur_pos = block_cache.first;
    auto prev_pos = kPageInvalidOffset;
    auto next_pos = kPageInvalidOffset;

    // 查找是否存在可合并的块
    auto cur_prev_pos = kPageInvalidOffset;
    while (cur_pos != kPageInvalidOffset) {
        auto del = false;
        auto cur_free_block = reinterpret_cast<FreeBlock*>(&block_cache.page[cur_pos]);
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
            if (block_cache.first == cur_pos) {
                block_cache.first = cur_free_block->next;
            }
            else {
                auto cur_prev_free_block = reinterpret_cast<FreeBlock*>(&block_cache.page[cur_prev_pos]);
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
        auto prev_free_block = reinterpret_cast<FreeBlock*>(&block_cache.page[prev_pos]);
        free_size += prev_free_block->size;
        free_pos = prev_pos;
    }
    if (next_pos != kPageInvalidOffset) {
        auto next_free_block = reinterpret_cast<FreeBlock*>(&block_cache.page[next_pos]);
        free_size += next_free_block->size;
    }

    assert(free_size >= sizeof(FreeBlock));
    auto free_free_block = reinterpret_cast<FreeBlock*>(&block_cache.page[free_pos]);
    free_free_block->next = block_cache.first;
    free_free_block->size = free_size;

    block_cache.first = free_pos;
    if (free_size > record_element.max_free_size) {
        record_element.max_free_size = free_size;
    }
}

void BlockManager::Print() {
    auto& pager = node_operator_->btree().bucket().pager();
    auto record_page = pager.Reference(block_info_->pgid, false);
    auto& record_cache = record_page.page_content<BlockPage>();
    auto record_arr = record_cache.record_arr;
    for (uint16_t i = 0; i < block_info_->count; i++) {

        auto alloc_page = pager.Reference(record_arr[i].pgid, false);
        auto& alloc_cache = alloc_page.page_content<BlockPage>();

        auto j = 0;
        auto prev_pos = kPageInvalidOffset;
        auto cur_pos = alloc_cache.first;
        while (cur_pos != kPageInvalidOffset) {
            auto free_free_block = reinterpret_cast<FreeBlock*>(&alloc_cache.page[cur_pos]);
            std::cout << std::format("[FreeBlock][pgid:{}][pos:{}]:{}", record_arr[i].pgid, cur_pos, free_free_block->size) << std::endl;
            prev_pos = cur_pos;
            cur_pos = free_free_block->next;
            ++j;
        }
        if (j == 0) {
            std::cout << std::format("[FreeBlock][pgid:{}]:null", record_arr[i].pgid) << std::endl;
        }
    }
}


void BlockManager::Build() {
    auto& pager = node_operator_->btree().bucket().pager();

    block_info_->pgid = pager.Alloc(1);
    block_info_->entry_index = 0;
    block_info_->count = 1;

    auto page_ref = pager.Reference(block_info_->pgid, true);
    auto& block_page = PageBuild(&page_ref);

    RecordBuild(&block_page.record_arr[0], 0, &page_ref);
}

void BlockManager::Clear() {
    if (block_info_->pgid == kPageInvalidId) {
        return;
    }
    RecordPageCopy();

    auto& pager = node_operator_->btree().bucket().pager();
    auto record_page = pager.Reference(block_info_->pgid, true);
    auto& record_cache = record_page.page_content<BlockPage>();
    auto record_arr = record_cache.record_arr;
    for (uint16_t i = 0; i < block_info_->count; i++) {
        pager.Free(record_arr->pgid, 1);
    }
    block_info_->pgid = kPageInvalidId;
}


void BlockManager::RecordBuild(BlockTableEntry* record_element, uint16_t record_index, PageReference* new_page_ref) {
    auto& pager = node_operator_->btree().bucket().pager();
    record_element->pgid = new_page_ref->page_id();
    record_element->max_free_size = 0;
    auto& block_page = new_page_ref->page_content<BlockPage>();
    PageSpaceOperator spacer{ &pager, &block_page.page_space };
    if (record_element->pgid == node_operator_->node().block_info.pgid) {
        spacer.AllocLeft(sizeof(BlockTableEntry));
        return;
    }
    auto rest_size = spacer.rest_size();
    auto offset = spacer.AllocLeft(rest_size);
    assert(offset.has_value());
    Free({ record_index, *offset, rest_size });
}

void BlockManager::RecordUpdateMaxFreeSize(BlockTableEntry* record_element, BlockPage* cache) {
    auto max_free_size = 0;
    auto cur_pos = cache->first;
    while (cur_pos != kPageInvalidOffset) {
        auto free_block = reinterpret_cast<FreeBlock*>(&cache->page[cur_pos]);
        if (free_block->size > max_free_size) {
            max_free_size = free_block->size;
        }
        cur_pos = free_block->next;
    }
    record_element->max_free_size = max_free_size;
}

void BlockManager::RecordPageCopy() {
    auto& bucket = node_operator_->btree().bucket();
    auto& pager = bucket.pager();
    auto& tx = bucket.tx();

    auto page = pager.Reference(block_info_->pgid, false);
    auto& cache = page.page_content<BlockPage>();
    if (tx.NeedCopy(cache.last_modified_txid)) {
        auto new_page = pager.Copy(std::move(page));
        auto& new_cache = page.page_content<BlockPage>();
        auto record_arr = new_cache.record_arr;
        new_cache.last_modified_txid = tx.txid();
        block_info_->pgid = page.page_id();
        record_arr[block_info_->entry_index].pgid = page.page_id();
    }
}


void BlockManager::PageAppend(PageReference* record_page) {
    assert(sizeof(BlockTableEntry) * (block_info_->count + 1) <= MaxSize());

    auto& pager = node_operator_->btree().bucket().pager();

    auto new_pgid = pager.Alloc(1);
    auto new_page = pager.Reference(new_pgid, true);
    auto& new_block_page = PageBuild(&new_page);
    
    auto* record_block_page = &record_page->page_content<BlockPage>();
    PageSpaceOperator record_spacer{ &pager, &record_block_page->page_space };
    BlockTableEntry* record_arr = record_block_page->record_arr;
    if (!record_spacer.AllocLeft(sizeof(BlockTableEntry))) {
        // 分配失败的话，将位于Left区间的record移动到新页中
        BlockTableEntry* new_record_arr = new_block_page.record_arr;
        auto byte_size = sizeof(BlockTableEntry) * block_info_->count;
        std::memcpy(new_record_arr, record_arr, byte_size);

        // 释放Left区间，分配到Right区间
        record_spacer.FreeLeft(byte_size);
        auto rest_size = record_spacer.rest_size();
        auto res = record_spacer.AllocRight(rest_size);
        assert(res.has_value());

        block_info_->pgid = new_pgid;
        Free({ block_info_->entry_index, *res, rest_size });

        block_info_->entry_index = block_info_->count;

        PageSpaceOperator spacer{ &pager, &new_block_page.page_space };
        spacer.AllocLeft(sizeof(BlockTableEntry) * block_info_->count);
        RecordBuild(&new_record_arr[block_info_->count], block_info_->count , &new_page);
    }
    else {
        RecordBuild(&record_arr[block_info_->count], block_info_->count, &new_page);
    }
    ++block_info_->count;
}

void BlockManager::PageCopy(BlockTableEntry* record_element) {
    auto& bucket = node_operator_->btree().bucket();
    auto& pager = bucket.pager();
    auto& tx = bucket.tx();

    auto record_page = pager.Reference(record_element->pgid, false);
    auto& record_cache = record_page.page_content<BlockPage>();
    if (tx.NeedCopy(record_cache.last_modified_txid)) {
        auto new_page = pager.Copy(std::move(record_page));
        record_element->pgid = new_page.page_id();
    }
}

BlockPage& BlockManager::PageBuild(PageReference* page_ref) {
    auto& pager = node_operator_->btree().bucket().pager();
    auto& block_page = page_ref->page_content<BlockPage>();

    block_page.last_modified_txid = node_operator_->btree().bucket().tx().txid();
    block_page.fragment_size = 0;
    block_page.first = kPageInvalidOffset;

    PageSpaceOperator spacer{ &pager, &block_page.page_space };
    spacer.Build();
    spacer.AllocLeft(sizeof(BlockPage) - sizeof(BlockPage::record_arr));

    return block_page;
}


} // namespace yudb