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

static BlockPage* PageToBlockPageCache(PageReferencer* page) {
    return reinterpret_cast<BlockPage*>(page->page_cache());
}


void Blocker::BlockInfoBuild() {
    auto& pager = noder_->btree().bucket().pager();
    block_info_->record_pgid = pager.Alloc(1);
    block_info_->record_index = 0;
    block_info_->record_offset = 0;
    block_info_->record_count = 1;
    block_info_->last_modified_txid = noder_->btree().bucket().tx().txid();

    auto page = pager.Reference(block_info_->record_pgid);
    auto cache = PageToBlockPageCache(&page);

    auto record_arr = reinterpret_cast<BlockRecord*>(&cache->data[0]);
    BlockRecordBuild(&record_arr[0], &page, sizeof(BlockRecord));
}

void Blocker::BlockInfoClear() {
    if (block_info_->record_pgid == kPageInvalidId) {
        return;
    }
    BlockRecordPageCopy();

    auto& pager = noder_->btree().bucket().pager();
    auto page = pager.Reference(block_info_->record_pgid);
    auto cache = PageToBlockPageCache(&page);
    auto record_arr = reinterpret_cast<BlockRecord*>(&cache->data[block_info_->record_offset]);
    for (uint16_t i = 0; i < block_info_->record_count; i++) {
        pager.Free(record_arr->pgid, 1);
    }
    block_info_->record_pgid = kPageInvalidId;
}


void Blocker::BlockRecordBuild(BlockRecord* record_element, PageReferencer* page, uint16_t header_block_size) {
    auto& pager = noder_->btree().bucket().pager();

    header_block_size = Alignment(header_block_size);

    auto cache = PageToBlockPageCache(page);
    record_element->pgid = page->page_id();
    if (header_block_size < BlockMaxSize()) {
        assert(header_block_size + sizeof(BlockRecord) <= BlockMaxSize());

        cache->first = header_block_size;
        record_element->max_free_size = BlockMaxSize() - header_block_size;

        auto free_block = reinterpret_cast<FreeBlock*>(&cache->data[header_block_size]);
        free_block->next = kFreeInvalidPos;
        free_block->size = record_element->max_free_size;
    }
    else {
        assert(header_block_size == BlockMaxSize());

        cache->first = kFreeInvalidPos;
        record_element->max_free_size = 0;
    }
    cache->last_modified_txid = noder_->btree().bucket().tx().txid();
}

void Blocker::BlockRecordUpdateMaxFreeSize(BlockRecord* record_element, BlockPage* cache) {
    auto max_free_size = 0;
    auto cur_pos = cache->first;
    while (cur_pos != kFreeInvalidPos) {
        auto free_block = reinterpret_cast<FreeBlock*>(&cache->data[cur_pos]);
        if (free_block->size > max_free_size) {
            max_free_size = free_block->size;
        }
        cur_pos = free_block->next;
    }
    record_element->max_free_size = max_free_size;
}

void Blocker::BlockRecordPageCopy() {
    auto& tx = noder_->btree().bucket().tx();
    if (tx.NeedCopy(block_info_->last_modified_txid)) {
        auto& pager = noder_->btree().bucket().pager();
        auto new_page = pager.Copy(block_info_->record_pgid);
        auto new_cache = PageToBlockPageCache(&new_page);
        auto record_arr = reinterpret_cast<BlockRecord*>(&new_cache->data[block_info_->record_offset]);

        block_info_->record_pgid = new_page.page_id();
        block_info_->last_modified_txid = tx.txid();

        new_cache->last_modified_txid = block_info_->last_modified_txid;
        record_arr[block_info_->record_index].pgid = new_page.page_id();
    }
}


void Blocker::BlockPageAppend(PageReferencer* record_page) {
    auto& pager = noder_->btree().bucket().pager();

    auto new_pgid = pager.Alloc(1);
    auto new_page = pager.Reference(new_pgid);
    auto new_cache = PageToBlockPageCache(&new_page);

    auto record_arr_size = sizeof(BlockRecord) * block_info_->record_count;
    // 即将被释放，保存block_record数组
    std::vector<BlockRecord> temp_record_arr{ block_info_->record_count };
    auto record_cache = PageToBlockPageCache(record_page);
    std::memcpy(temp_record_arr.data(),
        &record_cache->data[block_info_->record_offset],
        record_arr_size
    );
    // 自此开始先使用保存的block_record数组
    BlockFree({ block_info_->record_index, block_info_->record_offset, record_arr_size }, &temp_record_arr[block_info_->record_index]);
    record_arr_size += sizeof(BlockRecord);
    assert(record_arr_size <= BlockMaxSize());
    auto record_alloc = BlockAlloc(record_arr_size, temp_record_arr.data());

    BlockRecord* record_arr;
    uint16_t header_block_size;
    if (!record_alloc) {
        // 分配失败则使用新页面
        block_info_->record_pgid = new_pgid;
        block_info_->record_index = block_info_->record_count;
        block_info_->record_offset = 0;

        record_arr = reinterpret_cast<BlockRecord*>(new_cache->data);
        header_block_size = record_arr_size;
    }
    else {
        block_info_->record_index = record_alloc->first;
        block_info_->record_offset = record_alloc->second;
        block_info_->record_pgid = temp_record_arr[block_info_->record_index].pgid;
        auto page = pager.Reference(block_info_->record_pgid);
        auto cache = PageToBlockPageCache(&page);
        record_arr = reinterpret_cast<BlockRecord*>(&cache->data[block_info_->record_offset]);
        header_block_size = 0;
    }
    std::memcpy(record_arr, temp_record_arr.data(), record_arr_size - sizeof(BlockRecord));

    auto& tail_block_info_record = record_arr[block_info_->record_count];
    BlockRecordBuild(&tail_block_info_record, &new_page, header_block_size);
    ++block_info_->record_count;
}

void Blocker::BlockPageCopy(BlockRecord* record_element) {
    auto& pager = noder_->btree().bucket().pager();
    auto& tx = noder_->btree().bucket().tx();
    auto page = pager.Reference(record_element->pgid);
    auto cache = PageToBlockPageCache(&page);
    if (tx.NeedCopy(cache->last_modified_txid)) {
        auto new_page = pager.Copy(std::move(page));
        record_element->pgid = new_page.page_id();
    }
}


std::optional<std::pair<uint16_t, PageOffset>> Blocker::BlockAlloc(PageSize size, BlockRecord* record_arr) {
    auto& pager = noder_->btree().bucket().pager();
    
    size = Alignment(size);
    assert(size <= BlockMaxSize());

    if (block_info_->record_pgid == kPageInvalidId) {
        BlockInfoBuild();
    }
    BlockRecordPageCopy();

    auto page = pager.Reference(block_info_->record_pgid);
    auto cache = PageToBlockPageCache(&page);

    PageOffset ret_offset = 0;
    bool alloc_new_page = false;
    if (!record_arr) {
        alloc_new_page = true;
        record_arr = reinterpret_cast<BlockRecord*>(&cache->data[block_info_->record_offset]);
    }
    for (uint16_t i = 0; i < block_info_->record_count; i++) {
        auto& record_element = record_arr[i];

        assert(Aligned(record_element.max_free_size));
        if (record_element.max_free_size >= size) {
            BlockPageCopy(&record_element);

            auto alloc_page = pager.Reference(record_element.pgid);
            auto alloc_cache = PageToBlockPageCache(&alloc_page);

            // 找到足够分配的FreeBlock
            auto prev_pos = kFreeInvalidPos;
            assert(alloc_cache->first != kFreeInvalidPos);
            assert(Aligned(alloc_cache->first));
            auto cur_pos = alloc_cache->first;
            while (cur_pos != kFreeInvalidPos) {
                auto free_block = reinterpret_cast<FreeBlock*>(&alloc_cache->data[cur_pos]);
                assert(Aligned(free_block->size));
                if (free_block->size >= size) {
                    break;
                }
                assert(free_block->next != kFreeInvalidPos);
                assert(Aligned(free_block->next));
                prev_pos = cur_pos;
                cur_pos = free_block->next;
            }

            auto free_block = reinterpret_cast<FreeBlock*>(&alloc_cache->data[cur_pos]);
            auto new_pos = kFreeInvalidPos;
            auto new_size = free_block->size - size;
            if (new_size == 0) {
                new_pos = free_block->next;
            }
            else {
                assert(new_size >= sizeof(FreeBlock));
                new_pos = cur_pos + size;
                auto new_free_block = reinterpret_cast<FreeBlock*>(&alloc_cache->data[new_pos]);
                new_free_block->next = free_block->next;
                new_free_block->size = new_size;
            }

            if (alloc_cache->first == cur_pos) {
                alloc_cache->first = new_pos;
            }
            else {
                auto prev_free_block = reinterpret_cast<FreeBlock*>(&alloc_cache->data[prev_pos]);
                prev_free_block->next = new_pos;
            }

            if (free_block ->size == record_element.max_free_size) {
                BlockRecordUpdateMaxFreeSize(&record_element, alloc_cache);
            }
            return std::pair{ i, cur_pos };
        }
    }
    if (!alloc_new_page) return {};
    
    // 没有足够分配的空间，分配新页
    BlockPageAppend(&page);
    return BlockAlloc(size);
}

void Blocker::BlockFree(const std::tuple<uint16_t, PageOffset, PageSize>& free_block, BlockRecord* record_element) {
    BlockRecordPageCopy();

    auto& pager = noder_->btree().bucket().pager();

    auto [record_index, free_pos, free_size] = free_block;
    free_size = Alignment(free_size);

    if (!record_element) {
        auto record_page = pager.Reference(block_info_->record_pgid);
        auto record_cache = PageToBlockPageCache(&record_page);
        auto record_arr = reinterpret_cast<BlockRecord*>(&record_cache->data[block_info_->record_offset]);
        record_element = &record_arr[record_index];
    }

    BlockPageCopy(record_element);
    
    auto block_page = pager.Reference(record_element->pgid);
    auto block_cache = PageToBlockPageCache(&block_page);

    auto cur_pos = block_cache->first;
    auto prev_pos = kFreeInvalidPos;
    auto next_pos = kFreeInvalidPos;

    // 查找是否存在可合并的块
    auto cur_prev_pos = kFreeInvalidPos;
    while (cur_pos != kFreeInvalidPos) {
        assert(Aligned(cur_pos));
        auto del = false;
        auto cur_free_block = reinterpret_cast<FreeBlock*>(&block_cache->data[cur_pos]);
        assert(Aligned(cur_free_block->size));
        if (cur_pos + cur_free_block->size == free_pos) {
            assert(prev_pos == kFreeInvalidPos);
            prev_pos = cur_pos;
            del = true;
        }
        else if (cur_pos == free_pos + free_size) {
            assert(next_pos == kFreeInvalidPos);
            next_pos = cur_pos;
            del = true;
        }
        if (del) {
            // 先从链表中摘下
            if (block_cache->first == cur_pos) {
                block_cache->first = cur_free_block->next;
            }
            else {
                auto cur_prev_free_block = reinterpret_cast<FreeBlock*>(&block_cache->data[cur_prev_pos]);
                cur_prev_free_block->next = cur_free_block->next;
            }
            cur_pos = cur_prev_pos;     // 保持cur_prev_pos不变
        }
        if (prev_pos != kFreeInvalidPos && next_pos != kFreeInvalidPos) break;
        cur_prev_pos = cur_pos;
        cur_pos = cur_free_block->next;
    }

    // 合并
    if (prev_pos != kFreeInvalidPos) {
        auto prev_free_block = reinterpret_cast<FreeBlock*>(&block_cache->data[prev_pos]);
        free_size += prev_free_block->size;
        free_pos = prev_pos;
    }
    if (next_pos != kFreeInvalidPos) {
        auto next_free_block = reinterpret_cast<FreeBlock*>(&block_cache->data[next_pos]);
        free_size += next_free_block->size;
    }

    assert(free_size >= sizeof(FreeBlock));
    auto free_free_block = reinterpret_cast<FreeBlock*>(&block_cache->data[free_pos]);
    free_free_block->next = block_cache->first;
    free_free_block->size = free_size;

    block_cache->first = free_pos;
    if (free_size > record_element->max_free_size) {
        record_element->max_free_size = free_size;
    }
}

std::pair<uint8_t*, PageReferencer> Blocker::BlockLoad(uint16_t record_index, PageOffset offset) {
    auto& pager = noder_->btree().bucket().pager();

    auto page = pager.Reference(block_info_->record_pgid);
    auto cache = PageToBlockPageCache(&page);

    auto record_arr = reinterpret_cast<BlockRecord*>(&cache->data[block_info_->record_offset]);
    
    auto data_page = pager.Reference(record_arr[record_index].pgid);
    auto data_cache = PageToBlockPageCache(&data_page);
    return { &data_cache->data[offset], std::move(data_page) };
}

PageSize Blocker::BlockMaxSize() {
    auto size = noder_->btree().bucket().pager().page_size() - (sizeof(BlockPage) - sizeof(BlockPage::data));
    return size;
}


void Blocker::Print() {
    auto& pager = noder_->btree().bucket().pager();
    auto page = pager.Reference(block_info_->record_pgid);
    auto cache = PageToBlockPageCache(&page);
    auto record_arr = reinterpret_cast<BlockRecord*>(&cache->data[block_info_->record_offset]);
    for (uint16_t i = 0; i < block_info_->record_count; i++) {
        assert(Aligned(record_arr[i].max_free_size));

        auto alloc_page = pager.Reference(record_arr[i].pgid);
        auto alloc_cache = PageToBlockPageCache(&page);

        auto j = 0;
        auto prev_pos = kFreeInvalidPos;
        auto cur_pos = alloc_cache->first;
        while (cur_pos != kFreeInvalidPos) {
            auto free_free_block = reinterpret_cast<FreeBlock*>(&alloc_cache->data[cur_pos]);
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

} // namespace yudb