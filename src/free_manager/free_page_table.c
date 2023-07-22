#include <yudb/free_manager/free_page_table.h>

const uint16_t kFreePageStaticEntryIdOffset = 2;

LIBYUC_CONTAINER_STATIC_LIST_DEFINE(FreePage, PageOffset, FreePageEntry, YUDB_FREE_TABLE_REFERENCER, YUDB_FREE_TABLE_ACCESSOR, 2)


PageOffset FreePageTableGetMaxCount(PageOffset page_size) {
  return (page_size - (page_size / 4)) / sizeof(FreePageEntry);
}

PageOffset FreePageTableGetMaxFreeCount(FreePageTable* page_table) {
  return FreeTableBuddyGetMaxFreeCount(&page_table->buddy);
}

FreePageStaticList* FreePageTableGetStaticList(FreePageTable* page_table) {
  return (FreePageStaticList*)((uintptr_t)page_table + FreeTableBuddyGetMaxCount(&page_table->buddy));
}

void FreePageTableInit(FreePageTable* page_table, PageOffset page_size) {
  PageOffset max_count = FreePageTableGetMaxCount(page_size);
  FreeTableBuddyInit(&page_table->buddy, max_count);
  max_count -= kFreePageStaticEntryIdOffset;
  FreePageStaticList* static_list = FreePageTableGetStaticList(page_table);
  FreePageStaticListInit(static_list, max_count);
}

PageOffset FreePageTableAlloc(FreePageTable* page_table, PageOffset count) {
  return FreeTableBuddyAlloc(&page_table->buddy, count);
}

void FreePageTablePending(FreePageTable* page_table, PageOffset page_entry_id) {
   assert(page_entry_id != -1);
  FreePageStaticList* static_list = FreePageTableGetStaticList(page_table);
  FreePageEntry* page_entry = &static_list->obj_arr[page_entry_id - kFreePageStaticEntryIdOffset];
  if (page_entry->is_pending == false) {
    page_entry->is_pending = true;
    FreePageStaticListPush(static_list, kFreePageEntryListPending, page_entry_id - kFreePageStaticEntryIdOffset);
  }
}

void FreePageTableFree(FreePageTable* page_table, PageOffset page_entry_id) {
   assert(page_entry_id != -1);
  FreePageStaticList* static_list = FreePageTableGetStaticList(page_table);
  FreePageEntry* page_entry = &static_list->obj_arr[page_entry_id - kFreePageStaticEntryIdOffset];
  if (page_entry->is_pending) {
    page_entry->is_pending = false;
    // 将其从Pending链表中摘除
    PageOffset cur_id = FreePageStaticListIteratorFirst(static_list, kFreePageEntryListPending);
    PageOffset prev_id = YUDB_FREE_TABLE_REFERENCER_InvalidId;
    while (cur_id != YUDB_FREE_TABLE_REFERENCER_InvalidId) {
      if (cur_id == page_entry_id - kFreePageStaticEntryIdOffset) {
        FreePageStaticListDelete(static_list, kFreePageEntryListPending, prev_id, cur_id);
        break;
      }
      prev_id = cur_id;
      cur_id = FreePageStaticListIteratorNext(static_list, cur_id);
    }
  }
  FreeTableBuddyFree(&page_table->buddy, page_entry_id);
}
