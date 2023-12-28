#include "overflower.h"

#include "noder.h"
#include "pager.h"

#include "btree.h"
#include "tx.h"

namespace yudb {

using Block = FreeList::Block;

static PageSize Alignment(PageSize size) {
    if (size % sizeof(Block)) {
        size -= size % sizeof(Block);
        size += sizeof(Block);
    }
    return size;
}


void Overflower::RecordBuild(OverflowRecord* record_element,
    PageReferencer* page,
    uint16_t header_block_size
) {
    auto pager = noder_->pager();

    header_block_size = Alignment(header_block_size);

    record_element->pgid = page->page_id();
    if (header_block_size < pager->page_size()) {
        assert(header_block_size + sizeof(OverflowRecord) <= pager->page_size());

        record_element->free_list.first = header_block_size;
        record_element->free_list.max_free_size = pager->page_size() - header_block_size;

        auto cache = page->page_cache();

        auto block = reinterpret_cast<Block*>(&cache[header_block_size]);
        block->next = kFreeInvalidPos;
        block->size = record_element->free_list.max_free_size;
    }
    else {
        assert(header_block_size == pager->page_size());

        record_element->free_list.first = kFreeInvalidPos;
        record_element->free_list.max_free_size = 0;
    }
    record_element->last_modified_txid = noder_->btree()->update_tx()->txid();
}


void Overflower::RecordCopy() {
    if (noder_->btree()->update_tx()->IsExpiredTxId(overflow_info_->last_modified_txid)) {
        auto pager = noder_->pager();
        auto new_page = pager->Copy(overflow_info_->record_pgid);
        auto cache = new_page.page_cache();
        auto record_arr = reinterpret_cast<OverflowRecord*>(&cache[overflow_info_->record_offset]);

        overflow_info_->record_pgid = new_page.page_id();
        overflow_info_->last_modified_txid = noder_->btree()->update_tx()->txid();
        record_arr[overflow_info_->record_index].last_modified_txid = overflow_info_->last_modified_txid;
        record_arr[overflow_info_->record_index].pgid = new_page.page_id();
    }
}


void Overflower::OverflowPageBuild() {
    auto pager = noder_->pager();
    overflow_info_->record_pgid = pager->Alloc(1);
    overflow_info_->record_index = 0;
    overflow_info_->record_offset = 0;
    overflow_info_->record_count = 1;
    overflow_info_->last_modified_txid = noder_->btree()->update_tx()->txid();

    auto page = pager->Reference(overflow_info_->record_pgid);
    auto cache = page.page_cache();

    auto record_arr = reinterpret_cast<OverflowRecord*>(&cache[0]);
    RecordBuild(&record_arr[0], &page, sizeof(OverflowRecord));
}

void Overflower::OverflowPageAppend(PageReferencer* record_page) {
    auto pager = noder_->pager();

    auto new_pgid = pager->Alloc(1);
    auto new_page = pager->Reference(new_pgid);

    auto block_arr_size = sizeof(OverflowRecord) * overflow_info_->record_count;
    // 即将被释放，保存overflow_info_record数组
    std::vector<OverflowRecord> temp_record_arr{ overflow_info_->record_count };
    std::memcpy(temp_record_arr.data(),
        &record_page->page_cache()[overflow_info_->record_offset], 
        block_arr_size
    );
    // 自此开始先使用保存的overflow_info_record数组
    Free({ overflow_info_->record_index, overflow_info_->record_offset, block_arr_size }, &temp_record_arr[overflow_info_->record_index]);
    block_arr_size += sizeof(OverflowRecord);
    assert(block_arr_size <= pager->page_size());
    auto record_alloc = Alloc(block_arr_size, temp_record_arr.data());

    OverflowRecord* record_arr;
    uint16_t header_block_size;
    if (!record_alloc) {
        // 分配失败则使用新页面
        overflow_info_->record_pgid = new_pgid;
        overflow_info_->record_index = overflow_info_->record_count;
        overflow_info_->record_offset = 0;

        record_arr = reinterpret_cast<OverflowRecord*>(new_page.page_cache());
        header_block_size = block_arr_size;
    }
    else {
        overflow_info_->record_index = record_alloc->first;
        overflow_info_->record_offset = record_alloc->second;
        overflow_info_->record_pgid = temp_record_arr[overflow_info_->record_index].pgid;
        auto page = pager->Reference(overflow_info_->record_pgid);

        record_arr = reinterpret_cast<OverflowRecord*>(&page.page_cache()[overflow_info_->record_offset]);
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
    if (noder_->btree()->update_tx()->IsExpiredTxId(record_element->last_modified_txid)) {
        auto pager = noder_->pager();
        auto new_page = pager->Copy(record_element->pgid);
        record_element->pgid = new_page.page_id();
    }
    
}

void Overflower::RecordUpdateMaxFreeSize(OverflowRecord* record, uint8_t* cache) {
    auto max_free_size = 0;
    auto cur_pos = record->free_list.first;
    while (cur_pos != kFreeInvalidPos) {
        auto free_block = reinterpret_cast<Block*>(&cache[cur_pos]);
        if (free_block->size > max_free_size) {
            max_free_size = free_block->size;
        }
        cur_pos = free_block->next;
    }
    record->free_list.max_free_size = max_free_size;
}


std::optional<std::pair<uint16_t, PageOffset>> Overflower::Alloc(PageSize size, OverflowRecord* record_arr) {
    auto pager = noder_->pager();
    
    size = Alignment(size);
    
    if (overflow_info_->record_pgid == kPageInvalidId) {
        OverflowPageBuild();
    }
    RecordCopy();

    auto page = pager->Reference(overflow_info_->record_pgid);
    auto cache = page.page_cache();

    PageOffset ret_offset = 0;
    bool alloc_new_page = false;
    if (!record_arr) {
        alloc_new_page = true;
        record_arr = reinterpret_cast<OverflowRecord*>(&cache[overflow_info_->record_offset]);
    }
    for (uint16_t i = 0; i < overflow_info_->record_count; i++) {
        if (record_arr[i].free_list.max_free_size >= size) {
            assert(record_arr[i].free_list.first != kFreeInvalidPos);
            OverflowPageCopy(&record_arr[i]);

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

            if (free_block->size == record_arr[i].free_list.max_free_size) {
                RecordUpdateMaxFreeSize(&record_arr[i], alloc_cache);
            }

            return std::pair{ i, cur_pos };
        }
    }
    if (!alloc_new_page) return {};
    
    // 没有足够分配的空间，分配新页
    OverflowPageAppend(&page);

    return Alloc(size);
}

void Overflower::Free(const std::tuple<uint16_t, PageOffset, PageSize>& block, OverflowRecord* temp_record_element) {
    RecordCopy();

    auto pager = noder_->pager();

    auto page = pager->Reference(overflow_info_->record_pgid);
    auto cache = page.page_cache();

    auto [record_index, free_pos, free_size] = block;
    free_size = Alignment(free_size);

    auto record_arr = reinterpret_cast<OverflowRecord*>(&cache[overflow_info_->record_offset]);
    OverflowPageCopy(&record_arr[record_index]);

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

    if (!temp_record_element) {
        temp_record_element = &record_arr[record_index];
    }
    temp_record_element->free_list.first = free_pos;
    if (free_size > temp_record_element->free_list.max_free_size) {
        temp_record_element->free_list.max_free_size = free_size;
    }
}

std::pair<uint8_t*, PageReferencer> Overflower::Load(uint16_t record_index, PageOffset offset) {
    auto pager = noder_->pager();

    auto page = pager->Reference(overflow_info_->record_pgid);
    auto cache = page.page_cache();

    auto record_arr = reinterpret_cast<OverflowRecord*>(&cache[overflow_info_->record_offset]);
    
    auto data_page = pager->Reference(record_arr[record_index].pgid);
    auto data_cache = data_page.page_cache();
    return { &data_cache[offset], std::move(data_page) };
}




} // namespace yudb