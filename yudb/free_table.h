#ifndef YUDB_FREE_TABLE_H_
#define YUDB_FREE_TABLE_H_

#include <stdbool.h>
#include <stdint.h>

#include <CUtils/container/vector.h>
#include <CUtils/space_manager/buddy.h>
#include <CUtils/container/static_list.h>

#include <yudb/page.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus


/*
* 当前的单向静态链表队列切换较困难，故暂时不分Full队列
*/
CUTILS_SPACE_MANAGER_BUDDY_DECLARATION(Free, int16_t)

typedef enum {
	//kFree1EntryListFree = 0,
	kFree1EntryListPending = 1,
} Free1EntryListType;
CUTILS_CONTAINER_STATIC_LIST_DECLARATION_1(Free1, int16_t)
#pragma pack(1)
typedef struct _Free1Entry {
	int16_t entry_list_next;
	struct {
		uint8_t is_pending : 1;
		uint8_t : 7;
	};
} Free1Entry;
#pragma pack()
CUTILS_CONTAINER_STATIC_LIST_DECLARATION_2(Free1, int16_t, Free1Entry, 2)
typedef struct _Free1Table {
	FreeBuddy buddy;
} Free1Table;


typedef enum {
	//kFree0EntryListFree = 0,
	kFree0EntryListAlloc = 1,
	//kFree0EntryListFull = 2,
} Free0EntryListType;
CUTILS_CONTAINER_STATIC_LIST_DECLARATION_1(Free0, int16_t)
#pragma pack(1)
typedef struct _Free0Entry {
	struct {
		uint16_t read_select : 1;		// 读取是选择f1_0还是f1_1
		uint16_t write_select : 1;		// 写入是选择f1_0还是f1_1
		int16_t entry_list_next : 14;		// static_list 存储index，2^13
	};
	struct {
		uint8_t entry_list_type : 2;		// Free0EntryListType
		uint8_t free1_table_dirty : 1;		// f1是否为脏表
		uint8_t free1_table_pending : 1;	// f1是否存在pending
		uint8_t max_free_log : 4;		// f1最大连续空闲位，存储的是指数+1
	};
} Free0Entry;
#pragma pack()

CUTILS_CONTAINER_STATIC_LIST_DECLARATION_2(Free0, int16_t, Free0Entry, 4)

typedef struct _Free0Table {
	FreeBuddy buddy;
} Free0Table;

typedef struct _FreeTable {
	Free0Table* free0_table;
} FreeTable;

Free0StaticList* Free0TableGetStaticList(Free0Table* free0_table);
void Free0TableInit(Free0Table* free0_table, int16_t page_size);

void Free1TableInit(Free1Table* free1_table, int16_t page_size);
int16_t Free1TableAlloc(Free1Table* free1_table, int16_t count);
int16_t Free1TableGetMaxFreeCount(Free1Table* free1_table);
Free1StaticList* Free1TableGetStaticList(Free1Table* free0_table);

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

