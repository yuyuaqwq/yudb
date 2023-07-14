#include <yudb/free_manager/free_page_table.h>

const uint16_t kFreePageStaticEntryIdOffset = 2;

LIBYUC_CONTAINER_STATIC_LIST_DEFINE(FreePage, int16_t, FreePageEntry, YUDB_FREE_TABLE_REFERENCER, YUDB_FREE_TABLE_ACCESSOR, 2)


int16_t FreePageGetPageSize(FreePageTable* free_page_table) {
    return FreeTableBuddyGetMaxCount(&free_page_table->buddy) * 4;
}

int16_t FreePageTableGetMaxCount(int16_t page_size) {
    return (page_size - (page_size / 4)) / sizeof(FreePageEntry);
}

int16_t FreePageTableGetMaxFreeCount(FreePageTable* free_page_table) {
    return FreeTableBuddyGetMaxFreeCount(&free_page_table->buddy);
}

FreePageStaticList* FreePageTableGetStaticList(FreePageTable* free_page_table) {
    return (FreePageStaticList*)((uintptr_t)free_page_table + FreeTableBuddyGetMaxCount(&free_page_table->buddy));
}

void FreePageTableInit(FreePageTable* free_page_table, int16_t page_size) {
    int16_t max_count = FreePageTableGetMaxCount(page_size);
    FreeTableBuddyInit(&free_page_table->buddy, max_count);
    max_count -= kFreePageStaticEntryIdOffset;
    FreePageStaticListInit(FreePageTableGetStaticList(free_page_table), max_count);
}

int16_t FreePageTableAlloc(FreePageTable* free_page_table, int16_t count) {
    return FreeTableBuddyAlloc(&free_page_table->buddy, count);
}

void FreePageTablePending(FreePageTable* free_page_table, int16_t free1_entry_id) {
    assert(free1_entry_id != -1);
    FreePageStaticList* static_list = FreePageTableGetStaticList(free_page_table);
    FreePageEntry* free1_entry = &static_list->obj_arr[free1_entry_id - kFreePageStaticEntryIdOffset];
    free1_entry->is_pending = true;
    FreePageStaticListPush(static_list, kFreePageEntryListPending, free1_entry_id - kFreePageStaticEntryIdOffset);
}

void FreePageTableFree(FreePageTable* free_page_table, int16_t free1_entry_id) {
    assert(free1_entry_id != -1);
    FreePageStaticList* static_list = FreePageTableGetStaticList(free_page_table);
    FreePageEntry* free1_entry = &static_list->obj_arr[free1_entry_id - kFreePageStaticEntryIdOffset];
    if (free1_entry->is_pending) {
        free1_entry->is_pending = false;
        // 将其从Pending链表中摘除
        int16_t cur_id = FreePageStaticListIteratorFirst(static_list, kFreePageEntryListPending);
        int16_t prev_id = YUDB_FREE_TABLE_REFERENCER_InvalidId;
        while (cur_id != YUDB_FREE_TABLE_REFERENCER_InvalidId) {
            if (cur_id == free1_entry_id - kFreePageStaticEntryIdOffset) {
                FreePageStaticListDelete(static_list, kFreePageEntryListPending, prev_id, cur_id);
                break;
            }
            prev_id = cur_id;
            cur_id = FreePageStaticListIteratorNext(static_list, cur_id);
        }
    }
    FreeTableBuddyFree(&free_page_table->buddy, free1_entry_id);
}
