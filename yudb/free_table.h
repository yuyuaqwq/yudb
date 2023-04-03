#ifndef YUDB_FREE_TABLE_H_
#define YUDB_FREE_TABLE_H_

#include <stdbool.h>
#include <stdint.h>

#include <CUtils/container/vector.h>
#include <CUtils/container/space_manager.h>
#include <CUtils/container/static_list.h>

#include <yudb/page.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

typedef enum {
	kFree1EntryListFree = 0,
	kFree1EntryListPending = 1,
} Free1EntryListType;
typedef int32_t Free1Entry;
CUTILS_CONTAINER_SPACE_MANAGER_DECLARATION(Free1, int16_t, Free1Entry, 2)
typedef struct _Free1Table {
	Free1SpaceHead space_head;
} Free1Table;



typedef enum {
	kFree0EntryListFree = 0,
	kFree0EntryListAlloc = 1,
	kFree0EntryListFull = 2,
	kFree0EntryListPending = 3,
} Free0EntryListType;

typedef struct _Free0Entry {
	struct {
		uint16_t entry_list_type : 3;		// Free0EntryListType
		uint16_t entry_list_next : 13;		// static_list index
	};
	struct {
		uint16_t read_select : 1;		// 读取是选择f1_0还是f1_1
		uint16_t write_select : 1;		// 写入是选择f1_0还是f1_1
		uint16_t free1_table_dirty : 1;		// f1是否为脏表
		uint16_t max_free : 13;		// f1最大连续空闲位
	};
} Free0Entry;


CUTILS_CONTAINER_SPACE_MANAGER_DECLARATION(Free0, int16_t, Free0Entry, 4)
CUTILS_CONTAINER_STATIC_LIST_DECLARATION(Free0, int16_t, Free0Entry, 4)

typedef struct _Free0Table {
	union {
		Free0SpaceHead space_head;
		Free0StaticList static_list;
	};
} Free0Table;

typedef struct _FreeTable {
	Free0Table* free0_table;
} FreeTable;


void Free0TableInit(Free0Table* free0_table, int16_t page_size);


void Free1TableInit(Free1Table* free1_table, int16_t page_size);
int16_t Free1TableAlloc(Free1Table* free1_table, int16_t count);
int16_t Free1TableGetMaxFreeCount(Free1Table* free1_table);

PageId FreeTablePosToPageId(FreeTable* free_table, int16_t free0_entry_pos, int16_t free1_entry_pos);
void FreeTableGetPosFromPageId(FreeTable* free_table, PageId pgid, int16_t* free0_entry_pos, int16_t* free1_entry_pos);
bool FreeTableInit(FreeTable* table);
int16_t FreeTableAlloc(FreeTable* table, int16_t count, int16_t* free0_table_pos);
void FreeTableFree(FreeTable* table, PageId pgid, int16_t count);
void FreeTablePending(FreeTable* table, PageId pgid, int16_t count, PageId first_pgid);
void FreeTableFreePending(FreeTable* table, PageId first_pgid);
void FreeTableCleanPending(FreeTable* table);
bool FreeTableWrite(FreeTable* table, int32_t meta_index);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_FREE_TABLE_H_

