#include <yudb/free_manager/free_page_table.h>

const uint16_t kFreePageStaticEntryIdOffset = 2;

LIBYUC_CONTAINER_STATIC_LIST_DEFINE(FreePage, PageOffset, FreePageEntry, YUDB_FREE_TABLE_REFERENCER, YUDB_FREE_TABLE_ACCESSOR, 2)


PageOffset FreePageTableGetMaxCount(PageOffset page_size) {
    return (page_size - (page_size / 4)) / sizeof(FreePageEntry);
}

PageOffset FreePageTableGetMaxFreeCount(FreePageTable* free_page_table) {
    return FreeTableBuddyGetMaxFreeCount(&free_page_table->buddy);
}

FreePageStaticList* FreePageTableGetStaticList(FreePageTable* free_page_table) {
    return (FreePageStaticList*)((uintptr_t)free_page_table + FreeTableBuddyGetMaxCount(&free_page_table->buddy));
}

void FreePageTableInit(FreePageTable* free_page_table, PageOffset page_size) {
    PageOffset max_count = FreePageTableGetMaxCount(page_size);
    FreeTableBuddyInit(&free_page_table->buddy, max_count);
    max_count -= kFreePageStaticEntryIdOffset;
    FreePageStaticList* static_list = FreePageTableGetStaticList(free_page_table);
    FreePageStaticListInit(static_list, max_count);
}

PageOffset FreePageTableAlloc(FreePageTable* free_page_table, PageOffset count) {
    return FreeTableBuddyAlloc(&free_page_table->buddy, count);
}

void FreePageTablePending(FreePageTable* free_page_table, PageOffset entry_id) {
      assert(entry_id != -1);
    FreePageStaticList* static_list = FreePageTableGetStaticList(free_page_table);
    FreePageEntry* free1_entry = &static_list->obj_arr[entry_id - kFreePageStaticEntryIdOffset];
    free1_entry->is_pending = true;
    FreePageStaticListPush(static_list, kFreePageEntryListPending, entry_id - kFreePageStaticEntryIdOffset);
}

void FreePageTableFree(FreePageTable* free_page_table, PageOffset entry_id) {
    assert(entry_id != -1);
    FreePageStaticList* static_list = FreePageTableGetStaticList(free_page_table);
    FreePageEntry* free1_entry = &static_list->obj_arr[entry_id - kFreePageStaticEntryIdOffset];
    if (free1_entry->is_pending) {
        free1_entry->is_pending = false;
        // 将其从Pending链表中摘除
        PageOffset cur_id = FreePageStaticListIteratorFirst(static_list, kFreePageEntryListPending);
        PageOffset prev_id = YUDB_FREE_TABLE_REFERENCER_InvalidId;
        while (cur_id != YUDB_FREE_TABLE_REFERENCER_InvalidId) {
            if (cur_id == entry_id - kFreePageStaticEntryIdOffset) {
                FreePageStaticListDelete(static_list, kFreePageEntryListPending, prev_id, cur_id);
                break;
            }
            prev_id = cur_id;
            cur_id = FreePageStaticListIteratorNext(static_list, cur_id);
        }
    }
    FreeTableBuddyFree(&free_page_table->buddy, entry_id);
}
