#include "overflower.h"

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

static OverflowPage* PageToOverflowPageCache(PageReferencer* page) {
    return reinterpret_cast<OverflowPage*>(page->page_cache());
}



void Overflower::OverflowInfoBuild() {
    auto pager = noder_->pager();
    overflow_info_->record_pgid = pager->Alloc(1);
    overflow_info_->record_index = 0;
    overflow_info_->record_offset = 0;
    overflow_info_->record_count = 1;
    overflow_info_->last_modified_txid = noder_->btree()->update_tx()->txid();

    auto page = pager->Reference(overflow_info_->record_pgid);
    auto cache = PageToOverflowPageCache(&page);

    auto record_arr = reinterpret_cast<OverflowRecord*>(&cache->data[0]);
    RecordBuild(&record_arr[0], &page, sizeof(OverflowRecord));
}

void Overflower::OverflowInfoClear() {
    RecordCopy();

    auto pager = noder_->pager();
    auto page = pager->Reference(overflow_info_->record_pgid);
    auto cache = PageToOverflowPageCache(&page);
    auto record_arr = reinterpret_cast<OverflowRecord*>(&cache->data[overflow_info_->record_offset]);
    for (uint16_t i = 0; i < overflow_info_->record_count; i++) {
        pager->Free(record_arr->pgid, 1);
    }
    overflow_info_->record_pgid = kPageInvalidId;
}

void Overflower::RecordBuild(OverflowRecord* record_element,
    PageReferencer* page,
    uint16_t header_block_size
) {
    auto pager = noder_->pager();

    header_block_size = Alignment(header_block_size);

    auto cache = PageToOverflowPageCache(page);
    record_element->pgid = page->page_id();
    if (header_block_size < BlockMaxSize()) {
        assert(header_block_size + sizeof(OverflowRecord) <= BlockMaxSize());

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
    cache->last_modified_txid = noder_->btree()->update_tx()->txid();
}

void Overflower::RecordUpdateMaxFreeSize(OverflowRecord* record, OverflowPage* cache) {
    auto max_free_size = 0;
    auto cur_pos = cache->first;
    while (cur_pos != kFreeInvalidPos) {
        auto free_block = reinterpret_cast<FreeBlock*>(&cache->data[cur_pos]);
        if (free_block->size > max_free_size) {
            max_free_size = free_block->size;
        }
        cur_pos = free_block->next;
    }
    record->max_free_size = max_free_size;
}

void Overflower::RecordCopy() {
    if (noder_->btree()->update_tx()->IsExpiredTxId(overflow_info_->last_modified_txid)) {
        auto pager = noder_->pager();
        auto new_page = pager->Copy(overflow_info_->record_pgid);
        auto new_cache = PageToOverflowPageCache(&new_page);
        auto record_arr = reinterpret_cast<OverflowRecord*>(&new_cache->data[overflow_info_->record_offset]);

        overflow_info_->record_pgid = new_page.page_id();
        overflow_info_->last_modified_txid = noder_->btree()->update_tx()->txid();

        new_cache->last_modified_txid = overflow_info_->last_modified_txid;
        record_arr[overflow_info_->record_index].pgid = new_page.page_id();
    }
}


void Overflower::OverflowPageAppend(PageReferencer* record_page) {
    auto pager = noder_->pager();

    auto new_pgid = pager->Alloc(1);
    auto new_page = pager->Reference(new_pgid);
    auto new_cache = PageToOverflowPageCache(&new_page);

    auto block_arr_size = sizeof(OverflowRecord) * overflow_info_->record_count;
    // 即将被释放，保存overflow_record数组
    std::vector<OverflowRecord> temp_record_arr{ overflow_info_->record_count };
    auto record_cache = PageToOverflowPageCache(record_page);
    std::memcpy(temp_record_arr.data(),
        &record_cache->data[overflow_info_->record_offset],
        block_arr_size
    );
    // 自此开始先使用保存的overflow_record数组
    BlockFree({ overflow_info_->record_index, overflow_info_->record_offset, block_arr_size }, &temp_record_arr[overflow_info_->record_index]);
    block_arr_size += sizeof(OverflowRecord);
    assert(block_arr_size <= BlockMaxSize());
    auto record_alloc = BlockAlloc(block_arr_size, temp_record_arr.data());

    OverflowRecord* record_arr;
    uint16_t header_block_size;
    if (!record_alloc) {
        // 分配失败则使用新页面
        overflow_info_->record_pgid = new_pgid;
        overflow_info_->record_index = overflow_info_->record_count;
        overflow_info_->record_offset = 0;

        record_arr = reinterpret_cast<OverflowRecord*>(new_cache->data);
        header_block_size = block_arr_size;
    }
    else {
        overflow_info_->record_index = record_alloc->first;
        overflow_info_->record_offset = record_alloc->second;
        overflow_info_->record_pgid = temp_record_arr[overflow_info_->record_index].pgid;
        auto page = pager->Reference(overflow_info_->record_pgid);
        auto cache = PageToOverflowPageCache(&page);
        record_arr = reinterpret_cast<OverflowRecord*>(&cache->data[overflow_info_->record_offset]);
        header_block_size = 0;
    }
    std::memcpy(record_arr, temp_record_arr.data(), block_arr_size - sizeof(OverflowRecord));

    auto& tail_overflow_info_record = record_arr[overflow_info_->record_count];
    RecordBuild(&tail_overflow_info_record,
        &new_page,
        header_block_size
    );
    ++overflow_info_->record_count;
}

void Overflower::OverflowPageCopy(OverflowRecord* record_element) {
    auto pager = noder_->pager();
    auto page = pager->Reference(record_element->pgid);
    auto cache = PageToOverflowPageCache(&page);
    if (noder_->btree()->update_tx()->IsExpiredTxId(cache->last_modified_txid)) {
        auto new_page = pager->Copy(std::move(page));
        record_element->pgid = new_page.page_id();
    }
}


std::optional<std::pair<uint16_t, PageOffset>> Overflower::BlockAlloc(PageSize size, OverflowRecord* record_arr) {
    auto pager = noder_->pager();
    
    size = Alignment(size);
    
    assert(size <= BlockMaxSize());

    if (overflow_info_->record_pgid == kPageInvalidId) {
        OverflowInfoBuild();
    }
    RecordCopy();

    auto page = pager->Reference(overflow_info_->record_pgid);
    auto cache = PageToOverflowPageCache(&page);

    PageOffset ret_offset = 0;
    bool alloc_new_page = false;
    if (!record_arr) {
        alloc_new_page = true;
        record_arr = reinterpret_cast<OverflowRecord*>(&cache->data[overflow_info_->record_offset]);
    }
    for (uint16_t i = 0; i < overflow_info_->record_count; i++) {
        assert(Aligned(record_arr[i].max_free_size));
        if (record_arr[i].max_free_size >= size) {
            OverflowPageCopy(&record_arr[i]);

            auto alloc_page = pager->Reference(record_arr[i].pgid);
            auto alloc_cache = PageToOverflowPageCache(&alloc_page);

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

            if (free_block ->size == record_arr[i].max_free_size) {
                RecordUpdateMaxFreeSize(&record_arr[i], alloc_cache);
            }

            return std::pair{ i, cur_pos };
        }
    }
    if (!alloc_new_page) return {};
    
    // 没有足够分配的空间，分配新页
    OverflowPageAppend(&page);

    return BlockAlloc(size);
}

void Overflower::BlockFree(const std::tuple<uint16_t, PageOffset, PageSize>& free_block, OverflowRecord* temp_record_element) {
    RecordCopy();

    auto pager = noder_->pager();

    auto [record_index, free_pos, free_size] = free_block;
    free_size = Alignment(free_size);

    if (!temp_record_element) {
        auto record_page = pager->Reference(overflow_info_->record_pgid);
        auto record_cache = PageToOverflowPageCache(&record_page);
        auto record_arr = reinterpret_cast<OverflowRecord*>(&record_cache->data[overflow_info_->record_offset]);
        temp_record_element = &record_arr[record_index];
    }

    OverflowPageCopy(temp_record_element);
    
    auto overflow_page = pager->Reference(temp_record_element->pgid);
    auto overflow_cache = PageToOverflowPageCache(&overflow_page);

    auto cur_pos = overflow_cache->first;
    auto prev_pos = kFreeInvalidPos;
    auto next_pos = kFreeInvalidPos;

    // 查找是否存在可合并的块
    auto cur_prev_pos = kFreeInvalidPos;
    while (cur_pos != kFreeInvalidPos) {
        assert(Aligned(cur_pos));
        auto del = false;
        auto cur_free_block = reinterpret_cast<FreeBlock*>(&overflow_cache->data[cur_pos]);
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
            if (overflow_cache->first == cur_pos) {
                overflow_cache->first = cur_free_block->next;
            }
            else {
                auto cur_prev_free_block = reinterpret_cast<FreeBlock*>(&overflow_cache->data[cur_prev_pos]);
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
        auto prev_free_block = reinterpret_cast<FreeBlock*>(&overflow_cache->data[prev_pos]);
        free_size += prev_free_block->size;
        free_pos = prev_pos;
    }
    if (next_pos != kFreeInvalidPos) {
        auto next_free_block = reinterpret_cast<FreeBlock*>(&overflow_cache->data[next_pos]);
        free_size += next_free_block->size;
    }

    assert(free_size >= sizeof(FreeBlock));
    auto free_free_block = reinterpret_cast<FreeBlock*>(&overflow_cache->data[free_pos]);
    free_free_block->next = overflow_cache->first;
    free_free_block->size = free_size;

    overflow_cache->first = free_pos;
    if (free_size > temp_record_element->max_free_size) {
        temp_record_element->max_free_size = free_size;
    }
}

std::pair<uint8_t*, PageReferencer> Overflower::BlockLoad(uint16_t record_index, PageOffset offset) {
    auto pager = noder_->pager();

    auto page = pager->Reference(overflow_info_->record_pgid);
    auto cache = PageToOverflowPageCache(&page);

    auto record_arr = reinterpret_cast<OverflowRecord*>(&cache->data[overflow_info_->record_offset]);
    
    auto data_page = pager->Reference(record_arr[record_index].pgid);
    auto data_cache = PageToOverflowPageCache(&data_page);
    return { &data_cache->data[offset], std::move(data_page) };
}

PageSize Overflower::BlockMaxSize() {
    auto size = noder_->pager()->page_size() - sizeof(OverflowPage);
    return size;
}


void Overflower::Print() {
    auto pager = noder_->pager();
    auto page = pager->Reference(overflow_info_->record_pgid);
    auto cache = PageToOverflowPageCache(&page);
    auto record_arr = reinterpret_cast<OverflowRecord*>(&cache->data[overflow_info_->record_offset]);
    for (uint16_t i = 0; i < overflow_info_->record_count; i++) {
        assert(Aligned(record_arr[i].max_free_size));

        auto alloc_page = pager->Reference(record_arr[i].pgid);
        auto alloc_cache = PageToOverflowPageCache(&page);

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