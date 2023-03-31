#ifndef YUDB_FREE_TABLE_H_
#define YUDB_FREE_TABLE_H_

#include <stdbool.h>
#include <stdint.h>

#include <CUtils/container/vector.h>
#include <CUtils/container/space_manager.h>
#include <CUtils/container/bitmap.h>

#include <yudb/page.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

typedef struct _Free0Entry {
	struct {
		uint16_t free1_table_read_select : 1;		// 读取是选择f0还是f1
		uint16_t free1_table_write_select : 1;		// 写入是选择f0还是f1
		uint16_t free1_table_pending : 1;	// 该f1是否存在pending
		uint16_t free1_table_dirty : 1;		// 是否为脏条目
		uint16_t : 12;
	};
	int16_t free1_table_max_free;		// f1最大连续空闲位
} Free0Entry;

CUTILS_CONTAINER_SPACE_MANAGER_DECLARATION(Free0, int16_t, Free0Entry, 1)

typedef struct _FreeTable {
	Free0SpaceHead space_head;
} FreeTable;

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

