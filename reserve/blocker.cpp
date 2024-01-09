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


uint16_t Blocker::RecordIndexToArrayIndex(uint16_t record_index) {
    return noder_->node().block_record_count - record_index - 1;
}

void Blocker::BlockRecordClear() {
    auto record_count = noder_->node().block_record_count;
    if (record_count == 0) {
        return;
    }
    auto& pager = noder_->btree().bucket().pager();
    auto record_arr = noder_->BlockRecordArray();
    for (uint16_t i = 0; i < record_count; i++) {
        pager.Free(record_arr->pgid, 1);
    }
    noder_->node().block_record_count = 0;
}


void Blocker::RecordBuild(BlockRecord* record_element,
    PageReferencer* page,
    uint16_t header_block_size
) {
    auto& pager = noder_->btree().bucket().pager();

    header_block_size = Alignment(header_block_size);

    auto cache = page->page_cache();
    record_element->pgid = page->page_id();
    if (header_block_size < pager.page_size()) {
        record_element->first = header_block_size;
        record_element->max_free_size = pager.page_size() - header_block_size;

        auto free_block = reinterpret_cast<FreeBlock*>(&cache[header_block_size]);
        free_block->next = kFreeInvalidPos;
        free_block->size = record_element->max_free_size;
    }
    else {
        record_element->first = kFreeInvalidPos;
        record_element->max_free_size = 0;
    }
    record_element->last_modified_txid = noder_->btree().bucket().tx().txid();
}

void Blocker::RecordUpdateMaxFreeSize(BlockRecord* record, uint8_t* cache) {
    auto max_free_size = 0;
    auto cur_pos = record->first;
    while (cur_pos != kFreeInvalidPos) {
        auto free_block = reinterpret_cast<FreeBlock*>(&cache[cur_pos]);
        if (free_block->size > max_free_size) {
            max_free_size = free_block->size;
        }
        cur_pos = free_block->next;
    }
    record->max_free_size = max_free_size;
}


void Blocker::BlockPageAppend() {
    auto& pager = noder_->btree().bucket().pager();
    noder_->BlockRecordAlloc();
    auto new_page = pager.Reference(pager.Alloc(1));
    auto record_arr = noder_->BlockRecordArray();
    RecordBuild(&record_arr[0], &new_page, 0);
}

void Blocker::BlockPageCopy(BlockRecord* record_element) {
    auto& pager = noder_->btree().bucket().pager();
    auto& tx = noder_->btree().bucket().tx();
    auto page = pager.Reference(record_element->pgid);
    auto cache = page.page_cache();
    if (tx.IsExpiredTxId(record_element->last_modified_txid)) {
        auto new_page = pager.Copy(std::move(page));
        record_element->pgid = new_page.page_id();
    }
}


bool Blocker::BlockNeed(PageSize size) {
    auto& pager = noder_->btree().bucket().pager();
    size = Alignment(size);
    assert(size <= pager.page_size());

    auto record_arr = noder_->BlockRecordArray();;
    for (uint16_t i = 0; i < noder_->node().block_record_count; i++) {
        auto record_element = &record_arr[RecordIndexToArrayIndex(i)];
        if (record_element->max_free_size >= size) {
            return true;
        }
    }
    return noder_->node().free_size >= sizeof(BlockRecord);
}

std::pair<uint16_t, PageOffset> Blocker::BlockAlloc(PageSize size, bool alloc_new_page) {
    auto& pager = noder_->btree().bucket().pager();
    size = Alignment(size);
    assert(size <= pager.page_size());

    PageOffset ret_offset = 0;
    auto record_arr = noder_->BlockRecordArray();;
    for (uint16_t i = 0; i < noder_->node().block_record_count; i++) {
        auto record_element = &record_arr[RecordIndexToArrayIndex(i)];
        assert(Aligned(record_element->max_free_size));
        if (record_element->max_free_size >= size) {
            BlockPageCopy(record_element);

            auto alloc_page = pager.Reference(record_element->pgid);
            auto alloc_cache = alloc_page.page_cache();

            // 找到足够分配的FreeBlock
            auto prev_pos = kFreeInvalidPos;
            assert(record_element->first != kFreeInvalidPos);
            assert(Aligned(record_element->first));
            auto cur_pos = record_element->first;
            while (cur_pos != kFreeInvalidPos) {
                auto free_block = reinterpret_cast<FreeBlock*>(&alloc_cache[cur_pos]);
                assert(Aligned(free_block->size));
                if (free_block->size >= size) {
                    break;
                }
                assert(free_block->next != kFreeInvalidPos);
                assert(Aligned(free_block->next));
                prev_pos = cur_pos;
                cur_pos = free_block->next;
            }

            auto free_block = reinterpret_cast<FreeBlock*>(&alloc_cache[cur_pos]);
            auto new_pos = kFreeInvalidPos;
            auto new_size = free_block->size - size;
            if (new_size == 0) {
                new_pos = free_block->next;
            }
            else {
                assert(new_size >= sizeof(FreeBlock));
                new_pos = cur_pos + size;
                auto new_free_block = reinterpret_cast<FreeBlock*>(&alloc_cache[new_pos]);
                new_free_block->next = free_block->next;
                new_free_block->size = new_size;
            }

            if (record_element->first == cur_pos) {
                record_element->first = new_pos;
            }
            else {
                auto prev_free_block = reinterpret_cast<FreeBlock*>(&alloc_cache[prev_pos]);
                prev_free_block->next = new_pos;
            }

            if (free_block ->size == record_element->max_free_size) {
                RecordUpdateMaxFreeSize(record_element, alloc_cache);
            }
            return std::pair{ i, cur_pos };
        }
    }
    if (!alloc_new_page) return {};
    
    // 没有足够分配的空间，分配新页
    BlockPageAppend();
    return BlockAlloc(size, false);
}

void Blocker::BlockFree(const std::tuple<uint16_t, PageOffset, PageSize>& free_block) {
    auto& pager = noder_->btree().bucket().pager();

    auto [record_index, free_pos, free_size] = free_block;
    assert(record_index < noder_->node().block_record_count);
    free_size = Alignment(free_size);

    auto record_arr = noder_->BlockRecordArray();
    auto record_element = &record_arr[RecordIndexToArrayIndex(record_index)];

    BlockPageCopy(record_element);
    
    auto block_page = pager.Reference(record_element->pgid);
    auto block_cache = block_page.page_cache();

    auto cur_pos = record_element->first;
    auto prev_pos = kFreeInvalidPos;
    auto next_pos = kFreeInvalidPos;

    // 查找是否存在可合并的块
    auto cur_prev_pos = kFreeInvalidPos;
    while (cur_pos != kFreeInvalidPos) {
        assert(Aligned(cur_pos));
        auto del = false;
        auto cur_free_block = reinterpret_cast<FreeBlock*>(&block_cache[cur_pos]);
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
            if (record_element->first == cur_pos) {
                record_element->first = cur_free_block->next;
            }
            else {
                auto cur_prev_free_block = reinterpret_cast<FreeBlock*>(&block_cache[cur_prev_pos]);
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
        auto prev_free_block = reinterpret_cast<FreeBlock*>(&block_cache[prev_pos]);
        free_size += prev_free_block->size;
        free_pos = prev_pos;
    }
    if (next_pos != kFreeInvalidPos) {
        auto next_free_block = reinterpret_cast<FreeBlock*>(&block_cache[next_pos]);
        free_size += next_free_block->size;
    }

    assert(free_size >= sizeof(FreeBlock));
    auto free_free_block = reinterpret_cast<FreeBlock*>(&block_cache[free_pos]);
    free_free_block->next = record_element->first;
    free_free_block->size = free_size;

    record_element->first = free_pos;
    if (free_size > record_element->max_free_size) {
        record_element->max_free_size = free_size;
    }
}

std::pair<uint8_t*, PageReferencer> Blocker::BlockLoad(uint16_t record_index, PageOffset offset) {
    auto& pager = noder_->btree().bucket().pager();
    auto record_arr = noder_->BlockRecordArray();
    auto data_page = pager.Reference(record_arr[RecordIndexToArrayIndex(record_index)].pgid);
    auto data_cache = data_page.page_cache();
    return { &data_cache[offset], std::move(data_page) };
}


void Blocker::Print() {
    auto& pager = noder_->btree().bucket().pager();
    auto record_arr = noder_->BlockRecordArray();
    for (uint16_t i = 0; i < noder_->node().block_record_count; i++) {
        assert(Aligned(record_arr[i].max_free_size));

        auto alloc_page = pager.Reference(record_arr[i].pgid);
        auto alloc_cache = alloc_page.page_cache();

        auto j = 0;
        auto prev_pos = kFreeInvalidPos;
        auto cur_pos = record_arr[i].first;
        while (cur_pos != kFreeInvalidPos) {
            auto free_free_block = reinterpret_cast<FreeBlock*>(&alloc_cache[cur_pos]);
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