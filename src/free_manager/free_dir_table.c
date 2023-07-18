#include <yudb/free_manager/free_dir_table.h>

const PageOffset kFreeDirUnableToManageEntryCount = 3;

LIBYUC_CONTAINER_STATIC_LIST_DEFINE(FreeDir, PageOffset, FreeDirEntry, YUDB_FREE_TABLE_REFERENCER, YUDB_FREE_TABLE_ACCESSOR, 4)


PageOffset FreeDirTableGetMaxCount(PageOffset page_size) {
    return (page_size - (page_size / 4)) / sizeof(FreeDirEntry);
}

PageOffset FreeDirTableGetMaxFreeCount(FreeDirTable* free_dir_table) {
    return FreeTableBuddyGetMaxFreeCount(&free_dir_table->buddy);
}

FreeDirStaticList* FreeDirTableGetStaticList(FreeDirTable* dir_table) {
    return (FreeDirStaticList*)((uintptr_t)dir_table + FreeTableBuddyGetMaxCount(&dir_table->buddy));
}

void FreeDirTableInit(FreeDirTable* dir_table, FreeLevel level, PageOffset page_size) {
    PageOffset max_count = FreeDirTableGetMaxCount(page_size);
    FreeTableBuddyInit(&dir_table->buddy, max_count);
    max_count -= kFreeDirUnableToManageEntryCount;      // 末3项是管理不到的，故存在空洞
    FreeDirStaticList* static_list = FreeDirTableGetStaticList(dir_table);
    FreeDirStaticListInit(static_list, max_count);

    PageCount sub_max_free_page = FreeTableGetLevelPageCount(level + 1, page_size);

    for (PageCount i = 0; i < max_count; i++) {
        static_list->obj_arr[i].sub_max_free_log = LIBYUC_SPACE_MANAGER_BUDDY_TO_EXPONENT_OF_2(sub_max_free_page) + 1;
        static_list->obj_arr[i].read_select = 1;
        static_list->obj_arr[i].write_select = 0;
        static_list->obj_arr[i].sub_table_pending = false;
    }
}

PageOffset FreeDirTableAlloc(FreeDirTable* dir_table, PageOffset count) {
    PageOffset dir_entry_id = FreeTableBuddyAlloc(&dir_table->buddy, count);
    if (dir_entry_id != YUDB_FREE_TABLE_REFERENCER_InvalidId) {
        FreeDirStaticList* static_list = FreeDirTableGetStaticList(dir_table);
    }
    return dir_entry_id;
}

void FreeDirTablePending(FreeDirTable* dir_table, PageOffset dir_entry_id) {
      assert(dir_entry_id != -1);
    FreeDirStaticList* static_list = FreeDirTableGetStaticList(dir_table);
    FreeDirEntry* dir_entry = &static_list->obj_arr[dir_entry_id];
    dir_entry->sub_table_pending = true;
    FreeDirStaticListPush(static_list, kFreeDirEntryListPending, dir_entry_id);
}

void FreeDirTableFree(FreeDirTable* dir_table, PageOffset dir_entry_id) {
    FreeTableBuddyFree(&dir_table->buddy, dir_entry_id);
}

/*
* 从dir_table中查找足够分配的entry
*/
PageOffset FreeDirTableFindByPageCount(FreeDirTable* dir_table, PageCount page_count) {
    PageOffset dir_entry_prev_id = YUDB_FREE_TABLE_REFERENCER_InvalidId;
    FreeDirStaticList* static_list = FreeDirTableGetStaticList(dir_table);
    PageOffset dir_entry_id = FreeDirStaticListIteratorFirst(static_list, kFreeDirEntryListSubAlloc);
    while (true) {
        FreeDirEntry* free_dir_entry;
        if (dir_entry_id == YUDB_FREE_TABLE_REFERENCER_InvalidId) {
            dir_entry_id = FreeDirTableAlloc(dir_table, 1, false);
            // 在这里分配的，sub_table需要build
            if (dir_entry_id == YUDB_FREE_TABLE_REFERENCER_InvalidId) {
                return YUDB_FREE_TABLE_REFERENCER_InvalidId;
            }
            FreeDirStaticListPush(static_list, kFreeDirEntryListSubAlloc, dir_entry_id);
            // static_list->obj_arr[dir_entry_id].entry_list_type = kFreeDirEntryListSubAlloc;
        }
        free_dir_entry = &static_list->obj_arr[dir_entry_id];
        PageCount sub_max_free = LIBYUC_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(free_dir_entry->sub_max_free_log - 1);
        if (sub_max_free >= page_count) {
            break;
        }
        dir_entry_prev_id = dir_entry_id;
        dir_entry_id = FreeDirStaticListIteratorNext(static_list, dir_entry_id);
    }
    return dir_entry_id;
}
