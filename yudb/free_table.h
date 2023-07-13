#ifndef YUDB_FREE_TABLE_H_
#define YUDB_FREE_TABLE_H_

#include <stdbool.h>
#include <stdint.h>

#include <libyuc/container/vector.h>
#include <libyuc/space_manager/buddy.h>
#include <libyuc/container/static_list.h>

#include <yudb/page.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus


/*
* 当前的单向静态链表队列切换较困难，故暂时不分Full队列
*/
LIBYUC_SPACE_MANAGER_BUDDY_DECLARATION(Free, int16_t)

typedef enum {
    //kFreePageEntryListFree = 0,
    kFreePageEntryListPending = 1,
} FreePageEntryListType;
#pragma pack(1)
typedef struct _FreePageEntry {
    int16_t entry_list_next;
    struct {
        uint8_t is_pending : 1;
        uint8_t : 7;
    };
} FreePageEntry;
#pragma pack()
LIBYUC_CONTAINER_STATIC_LIST_DECLARATION(FreePage, int16_t, FreePageEntry, 2)
typedef struct _FreePageTable {
    FreeBuddy buddy;
} FreePageTable;


typedef enum {
    //kFreeDirEntryListFree = 0,
    kFreeDirEntryListAlloc = 1,
    //kFreeDirEntryListFull = 2,
} FreeDirEntryListType;
LIBYUC_CONTAINER_STATIC_LIST_DECLARATION_1(FreeDir, int16_t)
#pragma pack(1)
typedef struct _FreeDirEntry {
    struct {
        uint16_t read_select : 1;        // 读取是选择sub_0还是sub_1
        uint16_t write_select : 1;        // 写入是选择sub_0还是sub_1
        int16_t entry_list_next : 14;        // static_list 存储index，2^13
    };
    struct {
        // uint8_t entry_list_type : 2;        // FreeDirEntryListType
        uint8_t sub_table_dirty : 1;        // sub是否为脏表
        uint8_t sub_table_pending : 1;        // sub是否存在pending
        uint8_t sub_max_free_log : 5;        // sub最大连续空闲(下一级位x)page，存储的是指数+1
    };
} FreeDirEntry;
#pragma pack()

LIBYUC_CONTAINER_STATIC_LIST_DECLARATION_2(FreeDir, int16_t, FreeDirEntry, 4)

typedef struct _FreeDirTable {
    FreeBuddy buddy;
} FreeDirTable;

typedef struct _FreeTable {
    FreeDirTable* free0_table;
} FreeTable;

typedef enum {
    kFreeDirTable = 0,
    kFreePageTable = 1,
} FreeTableType;

FreeDirStaticList* FreeDirTableGetStaticList(FreeDirTable* dir_table);
void FreeDirTableInit(FreeDirTable* dir_table, int16_t page_size, int32_t sub_dir_level);

void FreePageTableInit(FreePageTable* page_table, int16_t page_size);
int16_t FreePageTableAlloc(FreePageTable* page_table, int16_t count);
int16_t FreePageTableGetMaxFreeCount(FreePageTable* page_table);
FreePageStaticList* FreePageTableGetStaticList(FreePageTable* page_table);

PageId FreeTablePosToPageId(FreeTable* free_table, int16_t free0_entry_pos, int16_t free1_entry_pos);
void FreeTableGetPosFromPageId(FreeTable* free_table, PageId pgid, int16_t* free0_entry_pos, int16_t* free1_entry_pos);
bool FreeTableInit(FreeTable* table);
int16_t FreeTableAlloc(FreeTable* table, int16_t count, int16_t* free0_table_pos);
void FreeTablePending(FreeTable* table, PageId pgid);
void FreeTableFree(FreeTable* table, PageId pgid);
void FreeTableCleanPending(FreeTable* table);
bool FreeTableWrite(FreeTable* table, int32_t meta_index);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_FREE_TABLE_H_

