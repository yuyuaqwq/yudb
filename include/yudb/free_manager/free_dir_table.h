#ifndef YUDB_FREE_MANAGER_FREE_DIR_TABLE_H_
#define YUDB_FREE_MANAGER_FREE_DIR_TABLE_H_

#include <stdbool.h>
#include <stdint.h>

#include <yudb/page.h>
#include <yudb/free_manager/free_table.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

typedef enum {
    //kFreeDirEntryListFree = 0,
    kFreeDirEntryListSubAlloc = 1,
    //kFreeDirEntryListSubFull = 2,
    kFreeDirEntryListPending = 3,
} FreeDirEntryListType;

LIBYUC_CONTAINER_STATIC_LIST_DECLARATION_1(FreeDir, PageOffset)
#pragma pack(1)
typedef struct _FreeDirEntry {
    struct {
        uint16_t read_select : 1;        // 读取是选择sub_0还是sub_1
        uint16_t write_select : 1;        // 写入是选择sub_0还是sub_1
        PageOffset entry_list_next : 14;        // static_list 存储index，2^13
    };
    struct {
        // uint8_t entry_list_type : 2;        // FreeDirEntryListType
        uint8_t sub_table_pending : 1;        // sub_table 是否存在/是否是 pending
        uint8_t sub_table_dirty : 1;        // sub_table是否为脏表
        uint8_t sub_max_free_log : 5;        // sub最大连续空闲page(不是下一级位)，存储的是指数+1
    };
} FreeDirEntry;
#pragma pack()
LIBYUC_CONTAINER_STATIC_LIST_DECLARATION_2(FreeDir, PageOffset, FreeDirEntry, 4)

typedef struct _FreeDirTable {
    FreeTableBuddy buddy;
} FreeDirTable;


PageOffset FreeDirTableGetMaxCount(PageOffset page_size);
PageOffset FreeDirTableGetMaxFreeCount(FreeDirTable* free_dir_table);
FreeDirStaticList* FreeDirTableGetStaticList(FreeDirTable* dir_table);
void FreeDirTableInit(FreeDirTable* dir_table, FreeLevel level, PageOffset page_size);
PageOffset FreeDirTableAlloc(FreeDirTable* dir_table, PageOffset count);
void FreeDirTablePending(FreeDirTable* dir_table, PageOffset dir_entry_id);
void FreeDirTableFree(FreeDirTable* dir_table, PageOffset dir_entry_id);
PageOffset FreeDirTableFindByPageCount(FreeDirTable* dir_table, PageCount page_count);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_FREE_MANAGER_FREE_DIR_TABLE_H_