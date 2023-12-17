#include "overflower.h"

#include "noder.h"
#include "pager.h"

namespace yudb {

using Record = Overflow::Record;
using Block = FreeList::Block;

static PageSize Alignment(PageSize size) {
    if (size % sizeof(Block)) {
        size -= size % sizeof(Block);
        size += sizeof(Block);
    }
    return size;
}


void Overflower::RecordBuild(Overflow::Record* record,
    PageReferencer* page,
    uint16_t header_block_size
) {
    header_block_size = Alignment(header_block_size);

    record->pgid = page->page_id();
    if (header_block_size < noder_->pager_->page_size()) {
        assert(header_block_size + sizeof(Overflow::Record) <= noder_->pager_->page_size());

        record->free_list.first = header_block_size;
        record->free_list.max_free_size = noder_->pager_->page_size() - header_block_size;

        auto cache = page->page_cache();

        auto block = reinterpret_cast<Block*>(&cache[header_block_size]);
        block->next = kFreeInvalidPos;
        block->size = record->free_list.max_free_size;
    }
    else {
        assert(header_block_size == noder_->pager_->page_size());

        record->free_list.first = kFreeInvalidPos;
        record->free_list.max_free_size = 0;
    }
}


void Overflower::OverflowBuild() {
    overflow_->record_pgid = noder_->pager_->Alloc(1);
    overflow_->record_offset = 0;
    overflow_->record_count = 1;
    auto page = noder_->pager_->Reference(overflow_->record_pgid);
    auto cache = page.page_cache();

    auto record_arr = reinterpret_cast<Record*>(&cache[0]);
    RecordBuild(&record_arr[0], &page, sizeof(Record));
}

void Overflower::OverflowAppend(PageReferencer* record_page) {
    auto pager = noder_->pager_;

    // 保存原始overflow_record数组
    std::vector<Record> record_arr;
    auto block_arr_size = sizeof(Record) * overflow_->record_count;
    memcpy(record_arr.data(), 
        &record_page->page_cache()[overflow_->record_offset], 
        block_arr_size
    );
    Free({ overflow_->record_pgid, overflow_->record_offset, block_arr_size });

    block_arr_size += sizeof(Record);
    assert(block_arr_size <= pager->page_size());
    auto record_alloc = Alloc(block_arr_size, false);
    if (!record_alloc) {
        // 当前的空闲overflow空间不足以扩展overflow数组了，新分配新页存放
        auto new_pgid = pager->Alloc(1);
        auto new_page = pager->Reference(new_pgid);

        overflow_->record_pgid = new_pgid;
        overflow_->record_offset = 0;

        memcpy(new_page.page_cache(), record_arr.data(), block_arr_size - sizeof(Record));
        auto record_arr = reinterpret_cast<Record*>(new_page.page_cache());
        auto& tail_overflow_record = record_arr[overflow_->record_count - 1];
        RecordBuild(&tail_overflow_record,
            &new_page,
            block_arr_size
        );
    }
    else {
        overflow_->record_pgid = record_alloc->first;
        overflow_->record_offset = record_alloc->second;
    }
    ++overflow_->record_count;
    
}


std::optional<std::pair<uint16_t, PageOffset>> Overflower::Alloc(PageSize size, bool alloc_new_page) {
    auto pager = noder_->pager_;
    
    size = Alignment(size);
    
    if (overflow_->record_pgid == kPageInvalidId) {
        OverflowBuild();
    }

    auto page = pager->Reference(overflow_->record_pgid);
    auto cache = page.page_cache();

    PageOffset ret_offset = 0;
    auto record_arr = reinterpret_cast<Record*>(&cache[overflow_->record_offset]);
    for (uint16_t i = 0; i < overflow_->record_count; i++) {
        if (record_arr[i].free_list.max_free_size >= size) {
            assert(record_arr[i].free_list.first != kFreeInvalidPos);
            auto alloc_page = pager->Reference(record_arr[i].pgid);
            auto alloc_cache = alloc_page.page_cache();

            // 找到足够分配的block
            auto prev_pos = kFreeInvalidPos;
            auto cur_pos = record_arr[i].free_list.first;
            while (cur_pos != kFreeInvalidPos) {
                auto free_block = reinterpret_cast<Block*>(&alloc_cache[cur_pos]);
                if (free_block->size >= size) {
                    break;
                }
                assert(free_block->next != kFreeInvalidPos);
                prev_pos = cur_pos;
                cur_pos = free_block->next;
            }

            auto free_block = reinterpret_cast<Block*>(&alloc_cache[cur_pos]);
            auto new_pos = kFreeInvalidPos;
            auto new_size = free_block->size - size;
            if (new_size == 0) {
                new_pos = free_block->next;
            }
            else {
                assert(new_size >= sizeof(Block));
                new_pos = cur_pos + size;
                auto new_free_block = reinterpret_cast<Block*>(&alloc_cache[new_pos]);
                new_free_block->next = free_block->next;
                new_free_block->size = new_size;
            }

            if (prev_pos == kFreeInvalidPos) {
                record_arr[i].free_list.first = new_pos;
            }
            else {
                auto prev_block = reinterpret_cast<Block*>(&alloc_cache[prev_pos]);
                prev_block->next = new_pos;
            }

            auto ret_offset = cur_pos;

            // 更新最大块
            auto max_free_size = 0;
            cur_pos = record_arr[i].free_list.first;
            while (cur_pos != kFreeInvalidPos) {
                free_block = reinterpret_cast<Block*>(&alloc_cache[cur_pos]);
                if (free_block->size > max_free_size) {
                    max_free_size = free_block->size;
                }
                cur_pos = free_block->next;
            }
            record_arr[i].free_list.max_free_size = max_free_size;

            return std::pair{ i, ret_offset };
        }
    }
    if (!alloc_new_page) return {};
    
    // 当前的overflow数组中没有足够分配的空间，分配新页
    OverflowAppend(&page);

    return Alloc(size);
}

void Overflower::Free(const std::tuple<uint16_t, PageOffset, PageSize>& block) {
    auto pager = noder_->pager_;

    auto page = pager->Reference(overflow_->record_pgid);
    auto cache = page.page_cache();

    auto [record_index, free_pos, free_size] = block;
    free_size = Alignment(free_size);

    auto record_arr = reinterpret_cast<Record*>(&cache[overflow_->record_offset]);

    auto cur_pos = record_arr[record_index].free_list.first;
    auto prev_pos = kFreeInvalidPos;
    auto next_pos = kFreeInvalidPos;
            
    // 查找是否存在可合并的块
    auto cur_prev_pos = kFreeInvalidPos;
    while (cur_pos != kFreeInvalidPos) {
        auto del = false;
        auto cur_block = reinterpret_cast<Block*>(&cache[cur_pos]);
        if (cur_pos + cur_block->size == free_pos) {
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
            if (cur_prev_pos == kFreeInvalidPos) {
                record_arr[record_index].free_list.first = cur_block->next;
            }
            else {
                auto cur_prev_block = reinterpret_cast<Block*>(&cache[cur_prev_pos]);
                cur_prev_block->next = cur_block->next;
            }
        }
        if (prev_pos != kFreeInvalidPos && next_pos != kFreeInvalidPos) break;
        cur_prev_pos = cur_pos;
        cur_pos = cur_block->next;
    }

    // 合并
    if (prev_pos != kFreeInvalidPos) {
        auto prev_block = reinterpret_cast<Block*>(&cache[prev_pos]);
        free_size += prev_block->size;
        free_pos = prev_pos;
    }
    if (next_pos != kFreeInvalidPos) {
        auto next_block = reinterpret_cast<Block*>(&cache[next_pos]);
        free_size += next_block->size;
    }

    assert(free_size >= sizeof(Block));
    auto free_block = reinterpret_cast<Block*>(&cache[free_pos]);
    free_block->next = record_arr[record_index].free_list.first;
    free_block->size = free_size;

    if (free_size > record_arr[record_index].free_list.max_free_size) {
        record_arr[record_index].free_list.max_free_size = free_size;
    }
    record_arr[record_index].free_list.first = free_pos;
}

std::pair<uint8_t*, PageReferencer> Overflower::Load(uint16_t record_index, PageOffset offset) {
    auto pager = noder_->pager_;

    auto page = pager->Reference(overflow_->record_pgid);
    auto cache = page.page_cache();

    auto record_arr = reinterpret_cast<Record*>(&cache[overflow_->record_offset]);
    
    auto data_page = pager->Reference(record_arr[record_index].pgid);
    auto data_cache = data_page.page_cache();
    return { &data_cache[offset], std::move(data_page) };
}


} // namespace yudb