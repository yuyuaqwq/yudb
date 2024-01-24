#include "blocker.h"

#include <format>

#include "noder.h"
#include "pager.h"

#include "btree.h"
#include "tx.h"

namespace yudb {

static PageSize Alignment(PageSize size) {
    if (size % sizeof(FreeBlock)) {
        size -= size % sizeof(FreeBlock);
        size += sizeof(FreeBlock);
    }
    return size;
}

static bool Aligned(PageSize size) {
    return size % sizeof(FreeBlock) == 0;
}

static PageOffset SpaceOffsetToBlockOffset(PageOffset offset) {
    return offset - sizeof(sizeof(BlockPage) - sizeof(BlockPage::data));
}


PageSize Blocker::MaxSize() {
    auto size = noder_->btree().bucket().pager().page_size() - (sizeof(BlockPage) - sizeof(BlockPage::data));
    return size;
}


std::pair<uint8_t*, PageReferencer> Blocker::Load(uint16_t record_index, PageOffset offset) {
    auto& pager = noder_->btree().bucket().pager();

    auto record_page = pager.Reference(block_info_->record_pgid, false);
    auto& record_cache = record_page.page_cache<BlockPage>();

    auto record_arr = reinterpret_cast<BlockRecord*>(&record_cache.data[0]);
    auto data_page = pager.Reference(record_arr[record_index].pgid, false);
    auto& data_cache = data_page.page_cache<BlockPage>();
    return { &data_cache.data[offset], std::move(data_page) };
}


std::optional<std::pair<uint16_t, PageOffset>> Blocker::Alloc(PageSize size) {
    auto& pager = noder_->btree().bucket().pager();

    size = Alignment(size);
    assert(size <= MaxSize());

    if (block_info_->record_pgid == kPageInvalidId) {
        InfoBuild();
    }
    RecordPageCopy();

    auto record_page = pager.Reference(block_info_->record_pgid, true);
    auto& record_cache = record_page.page_cache<BlockPage>();
    auto record_arr = reinterpret_cast<BlockRecord*>(&record_cache.data[0]);
    for (uint16_t i = 0; i < block_info_->record_count; i++) {
        auto& record_element = record_arr[i];
        assert(Aligned(record_element.max_free_size));
        if (record_element.pgid == record_page.page_id()) {
            // 是记录所在的页面，如果空间不足需要进行分配
            if (record_element.max_free_size < size) {
                PageSpacer spacer{ &pager, &record_cache.page_space };
                auto res = spacer.AllocRight(size);
                if (res) {
                    Free({ i, SpaceOffsetToBlockOffset(*res), size});
                }
            }
        }
        if (record_element.max_free_size >= size) {
            PageCopy(&record_element);
            auto alloc_page = pager.Reference(record_element.pgid, true);
            auto& alloc_cache = alloc_page.page_cache<BlockPage>();

            // 找到足够分配的FreeBlock
            auto prev_pos = kPageInvalidOffset;
            assert(alloc_cache.first != kPageInvalidOffset);
            assert(Aligned(alloc_cache.first));
            auto cur_pos = alloc_cache.first;
            while (cur_pos != kPageInvalidOffset) {
                auto free_block = reinterpret_cast<FreeBlock*>(&alloc_cache.data[cur_pos]);
                assert(Aligned(free_block->size));
                if (free_block->size >= size) {
                    break;
                }
                assert(free_block->next != kPageInvalidOffset);
                assert(Aligned(free_block->next));
                prev_pos = cur_pos;
                cur_pos = free_block->next;
            }

            auto free_block = reinterpret_cast<FreeBlock*>(&alloc_cache.data[cur_pos]);
            auto new_pos = kPageInvalidOffset;
            auto new_size = free_block->size - size;
            if (new_size == 0) {
                new_pos = free_block->next;
            }
            else {
                assert(new_size >= sizeof(FreeBlock));
                new_pos = cur_pos + size;
                auto new_free_block = reinterpret_cast<FreeBlock*>(&alloc_cache.data[new_pos]);
                new_free_block->next = free_block->next;
                new_free_block->size = new_size;
            }

            if (alloc_cache.first == cur_pos) {
                alloc_cache.first = new_pos;
            }
            else {
                auto prev_free_block = reinterpret_cast<FreeBlock*>(&alloc_cache.data[prev_pos]);
                prev_free_block->next = new_pos;
            }

            if (free_block->size == record_element.max_free_size) {
                RecordUpdateMaxFreeSize(&record_element, &alloc_cache);
            }
            return std::pair{ i, cur_pos };
        }
    }

    // 没有足够分配的空间，分配新页
    PageAppend(&record_page);
    return Alloc(size);
}

void Blocker::Free(const std::tuple<uint16_t, PageOffset, PageSize>& free_block) {
    RecordPageCopy();

    auto& pager = noder_->btree().bucket().pager();

    auto [record_index, free_pos, free_size] = free_block;
    assert(Aligned(free_pos));
    free_size = Alignment(free_size);

    auto record_page = pager.Reference(block_info_->record_pgid, true);
    auto& record_cache = record_page.page_cache<BlockPage>();
    auto record_arr = reinterpret_cast<BlockRecord*>(&record_cache.data[0]);
    auto& record_element = record_arr[record_index];


    PageCopy(&record_element);
    auto block_page = pager.Reference(record_element.pgid, true);
    auto& block_cache = block_page.page_cache<BlockPage>();

    auto cur_pos = block_cache.first;
    auto prev_pos = kPageInvalidOffset;
    auto next_pos = kPageInvalidOffset;

    // 查找是否存在可合并的块
    auto cur_prev_pos = kPageInvalidOffset;
    while (cur_pos != kPageInvalidOffset) {
        assert(Aligned(cur_pos));
        auto del = false;
        auto cur_free_block = reinterpret_cast<FreeBlock*>(&block_cache.data[cur_pos]);
        assert(Aligned(cur_free_block->size));
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
                auto cur_prev_free_block = reinterpret_cast<FreeBlock*>(&block_cache.data[cur_prev_pos]);
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
        auto prev_free_block = reinterpret_cast<FreeBlock*>(&block_cache.data[prev_pos]);
        free_size += prev_free_block->size;
        free_pos = prev_pos;
    }
    if (next_pos != kPageInvalidOffset) {
        auto next_free_block = reinterpret_cast<FreeBlock*>(&block_cache.data[next_pos]);
        free_size += next_free_block->size;
    }

    assert(free_size >= sizeof(FreeBlock));
    auto free_free_block = reinterpret_cast<FreeBlock*>(&block_cache.data[free_pos]);
    free_free_block->next = block_cache.first;
    free_free_block->size = free_size;

    block_cache.first = free_pos;
    if (free_size > record_element.max_free_size) {
        record_element.max_free_size = free_size;
    }
}

void Blocker::Print() {
    auto& pager = noder_->btree().bucket().pager();
    auto record_page = pager.Reference(block_info_->record_pgid, false);
    auto& record_cache = record_page.page_cache<BlockPage>();
    auto record_arr = reinterpret_cast<BlockRecord*>(&record_cache.data[0]);
    for (uint16_t i = 0; i < block_info_->record_count; i++) {
        assert(Aligned(record_arr[i].max_free_size));

        auto alloc_page = pager.Reference(record_arr[i].pgid, false);
        auto& alloc_cache = alloc_page.page_cache<BlockPage>();

        auto j = 0;
        auto prev_pos = kPageInvalidOffset;
        auto cur_pos = alloc_cache.first;
        while (cur_pos != kPageInvalidOffset) {
            auto free_free_block = reinterpret_cast<FreeBlock*>(&alloc_cache.data[cur_pos]);
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


void Blocker::InfoBuild() {
    auto& pager = noder_->btree().bucket().pager();
    block_info_->record_pgid = pager.Alloc(1);
    block_info_->record_index = 0;
    block_info_->record_count = 1;

    auto record_page = pager.Reference(block_info_->record_pgid, true);
    auto& record_cache = record_page.page_cache<BlockPage>();

    auto record_arr = reinterpret_cast<BlockRecord*>(&record_cache.data[0]);
    RecordBuild(&record_arr[0], &record_page);
    PageSpacer spacer{ &pager, &record_cache.page_space };
    spacer.AllocLeft(sizeof(BlockRecord));
}

void Blocker::InfoClear() {
    if (block_info_->record_pgid == kPageInvalidId) {
        return;
    }
    RecordPageCopy();

    auto& pager = noder_->btree().bucket().pager();
    auto record_page = pager.Reference(block_info_->record_pgid, true);
    auto& record_cache = record_page.page_cache<BlockPage>();
    auto record_arr = reinterpret_cast<BlockRecord*>(&record_cache.data[0]);
    for (uint16_t i = 0; i < block_info_->record_count; i++) {
        pager.Free(record_arr->pgid, 1);
    }
    block_info_->record_pgid = kPageInvalidId;
}


void Blocker::RecordBuild(BlockRecord* record_element, PageReferencer* page) {
    auto& pager = noder_->btree().bucket().pager();

    auto& cache = page->page_cache<BlockPage>();
    PageSpacer spacer{ &pager, &cache.page_space };
    spacer.Build();
    spacer.AllocLeft(sizeof(BlockPage) - sizeof(BlockPage::data));
    
    cache.last_modified_txid = noder_->btree().bucket().tx().txid();
    record_element->pgid = page->page_id();
    if (record_element->pgid == noder_->node().block_info.record_pgid) {
        cache.first = kPageInvalidOffset;
        record_element->max_free_size = 0;
    }
    else {
        cache.first = 0;
        auto free_block = reinterpret_cast<FreeBlock*>(&cache.data[0]);
        free_block->next = kPageInvalidOffset;
        free_block->size = spacer.rest_size();
        record_element->max_free_size = free_block->size;
        spacer.AllocLeft(record_element->max_free_size);
    }
}

void Blocker::RecordUpdateMaxFreeSize(BlockRecord* record_element, BlockPage* cache) {
    auto max_free_size = 0;
    auto cur_pos = cache->first;
    while (cur_pos != kPageInvalidOffset) {
        auto free_block = reinterpret_cast<FreeBlock*>(&cache->data[cur_pos]);
        if (free_block->size > max_free_size) {
            max_free_size = free_block->size;
        }
        cur_pos = free_block->next;
    }
    record_element->max_free_size = max_free_size;
}

void Blocker::RecordPageCopy() {
    //auto& tx = noder_->btree().bucket().tx();
    //if (tx.NeedCopy(block_info_->last_modified_txid)) {
    //    auto& pager = noder_->btree().bucket().pager();
    //    auto new_page = pager.Copy(block_info_->record_pgid);
    //    auto& new_cache = new_page.page_cache<BlockPage>();
    //    auto record_arr = reinterpret_cast<BlockRecord*>(&new_cache.data[block_info_->record_offset]);

    //    block_info_->record_pgid = new_page.page_id();
    //    block_info_->last_modified_txid = tx.txid();

    //    new_cache.last_modified_txid = block_info_->last_modified_txid;
    //    record_arr[block_info_->record_index].pgid = new_page.page_id();
    //}
}


void Blocker::PageAppend(PageReferencer* record_page) {
    auto& pager = noder_->btree().bucket().pager();

    auto new_pgid = pager.Alloc(1);
    auto new_page = pager.Reference(new_pgid, true);
    auto& new_cache = new_page.page_cache<BlockPage>();

    auto* record_cache = &record_page->page_cache<BlockPage>();
    PageSpacer record_spacer{ &pager, &record_cache->page_space };
    BlockRecord* record_arr = reinterpret_cast<BlockRecord*>(&record_cache->data[0]);
    if (!record_spacer.AllocLeft(sizeof(BlockRecord))) {
        // 分配失败的话，将位于Left区间的record移动到新页中
        block_info_->record_pgid = new_pgid;
        BlockRecord* new_record_arr = reinterpret_cast<BlockRecord*>(&new_cache.data[0]);
        auto byte_size = sizeof(BlockRecord) * block_info_->record_count;
        std::memcpy(new_record_arr, record_arr, byte_size);
        // 释放Left区间，分配到Right区间
        record_spacer.FreeLeft(byte_size);
        byte_size = Alignment(byte_size);
        auto res = record_spacer.AllocRight(byte_size);
        assert(res.has_value());
        assert(record_spacer.rest_size() == 0);
        Free({ block_info_->record_index, SpaceOffsetToBlockOffset(*res), byte_size });
        block_info_->record_index = block_info_->record_count;

        RecordBuild(&new_record_arr[block_info_->record_count++], &new_page);
    }
    else {
        RecordBuild(&record_arr[block_info_->record_count++], record_page);
    }
}

void Blocker::PageCopy(BlockRecord* record_element) {
    auto& pager = noder_->btree().bucket().pager();
    auto& tx = noder_->btree().bucket().tx();
    auto record_page = pager.Reference(record_element->pgid, false);
    auto& record_cache = record_page.page_cache<BlockPage>();
    if (tx.NeedCopy(record_cache.last_modified_txid)) {
        auto new_page = pager.Copy(std::move(record_page));
        record_element->pgid = new_page.page_id();
    }
}

} // namespace yudb