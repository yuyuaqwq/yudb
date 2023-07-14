#include <yudb/free_manager/free_dir_table.h>

const uint16_t kFreeDirUnableToManageEntryCount = 3;

LIBYUC_CONTAINER_STATIC_LIST_DEFINE(FreeDir, int16_t, FreeDirEntry, YUDB_FREE_TABLE_REFERENCER, YUDB_FREE_TABLE_ACCESSOR, 4)

int16_t FreeDirGetPageSize(FreeDirTable* free_dir_table) {
    return FreeTableBuddyGetMaxCount(&free_dir_table->buddy) * 4;
}

int16_t FreeDirTableGetMaxCount(int16_t page_size) {
    return (page_size - (page_size / 4)) / sizeof(FreeDirEntry);
}

int16_t FreeDirTableGetMaxFreeCount(FreeDirTable* free_dir_table) {
    return FreeTableBuddyGetMaxFreeCount(&free_dir_table->buddy);
}

FreeDirStaticList* FreeDirTableGetStaticList(FreeDirTable* free_dir_table) {
    return (FreeDirStaticList*)((uintptr_t)free_dir_table + FreeTableBuddyGetMaxCount(&free_dir_table->buddy));
}

void FreeDirTableInit(FreeDirTable* free_dir_table, int16_t page_size, uint32_t level) {
    int16_t max_count = FreeDirTableGetMaxCount(page_size);
    FreeTableBuddyInit(&free_dir_table->buddy, max_count);
    max_count -= kFreeDirUnableToManageEntryCount;
    FreeDirStaticList* static_list = FreeDirTableGetStaticList(free_dir_table);
    FreeDirStaticListInit(static_list, max_count);

    uint32_t sub_max_free_page = FreeTableGetPageCount(level, page_size);

    for (uint32_t i = 0; i < max_count; i++) {
        uint32_t aa = LIBYUC_SPACE_MANAGER_BUDDY_TO_EXPONENT_OF_2(sub_max_free_page);
        static_list->obj_arr[i].sub_max_free_log = LIBYUC_SPACE_MANAGER_BUDDY_TO_EXPONENT_OF_2(sub_max_free_page) + 1;
        static_list->obj_arr[i].read_select = 1;
        static_list->obj_arr[i].write_select = 0;
        static_list->obj_arr[i].sub_table_pending = false;
    }
    // 第0项-1，因为第0项包括了dir_table自身，无法分配整个entry[0]
    static_list->obj_arr[0].sub_max_free_log -= 1;
}

int16_t FreeDirTableAlloc(FreeDirTable* dir_table, int16_t count) {
    return FreeTableBuddyAlloc(&dir_table->buddy, count);
}

void FreeDirTableFree(FreeDirTable* dir_table, int16_t dir_entry_id) {
    FreeTableBuddyFree(&dir_table->buddy, dir_entry_id);
}

/*
* 从dir_table中查找足够分配的entry
*/
int16_t FreeDirTableFindBySubPageCount(FreeDirTable* dir_table, FreeDirEntry* parent_entry, uint32_t sub_page_count) {
    int16_t dir_entry_prev_id = YUDB_FREE_TABLE_REFERENCER_InvalidId;
    FreeDirStaticList* static_list = FreeDirTableGetStaticList(dir_table);
    int16_t dir_entry_id = FreeDirStaticListIteratorFirst(static_list, kFreeDirEntryListSubAlloc);
    while (true) {
        FreeDirEntry* free_dir_entry;
        if (dir_entry_id == YUDB_FREE_TABLE_REFERENCER_InvalidId) {
            dir_entry_id = FreeDirTableAlloc(dir_table, 1);
            if (dir_entry_id == YUDB_FREE_TABLE_REFERENCER_InvalidId) {
                return YUDB_FREE_TABLE_REFERENCER_InvalidId;
            }
            FreeDirStaticListPush(static_list, kFreeDirEntryListSubAlloc, dir_entry_id);
            // static_list->obj_arr[dir_entry_id].entry_list_type = kFreeDirEntryListSubAlloc;
        }
        free_dir_entry = &static_list->obj_arr[dir_entry_id];
        int32_t sub_max_free = LIBYUC_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(free_dir_entry->sub_max_free_log - 1);
        if (sub_max_free >= sub_page_count) {
            break;
        }
        dir_entry_prev_id = dir_entry_id;
        dir_entry_id = FreeDirStaticListIteratorNext(static_list, dir_entry_id);
    }
    return dir_entry_id;
}

void FreeDirTableUpdateEntryFreeCount(FreeDirEntry* free_dir_entry, void* sub_table) {
    free_dir_entry->sub_max_free_log = LIBYUC_SPACE_MANAGER_BUDDY_TO_EXPONENT_OF_2(FreeDirTableGetMaxFreeCount(sub_table)) + 1;
    if (free_dir_entry->sub_max_free_log != 0) {
        // 挂回可分配队列
        //FreeDirStaticListSwitch(static_list, kFreeDirEntryListSubFull, free0_entry_prev_id, free0_entry_id, kFreeDirEntryListAlloc);
    }
}