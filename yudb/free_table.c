#include "yudb/free_table.h"

#include "yudb/db_file.h"
#include "yudb/pager.h"
#include "yudb/yudb.h"


#define YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId (-1)
#define YUDB_FREE_TABLE_FREE_REFERENCER YUDB_FREE_TABLE_FREE_REFERENCER
CUTILS_CONTAINER_SPACE_MANAGER_DEFINE(Free0, int16_t, Free0Entry, YUDB_FREE_TABLE_FREE_REFERENCER, 1)


const PageId kMetaStartId = 0;
const PageId kFree0ListStartId = 2;
const PageId kFree1TableStartId = 4;


typedef enum {
	kFree1ListFree = 0,
	kFree1ListPending = 1,
} Free1ListType;
typedef int32_t Free1Entry;
CUTILS_CONTAINER_SPACE_MANAGER_DECLARATION(Free1, int16_t, Free1Entry, 2)
typedef struct _Free1Table {
	Free1SpaceHead space_head;
} Free1Table;


CUTILS_CONTAINER_SPACE_MANAGER_DEFINE(Free1, int16_t, Free1Entry, YUDB_FREE_TABLE_FREE_REFERENCER, 1)

static int16_t Free0TableGetMaxCount(FreeTable* free_table) {
	Pager* pager = ObjectGetFromField(free_table, Pager, free_table);
	return (pager->page_size - sizeof(Free0SpaceHead)) / sizeof(Free0Entry);
}

static int16_t Free1TableGetMaxCount(FreeTable* free_table) {
	Pager* pager = ObjectGetFromField(free_table, Pager, free_table);
	return (pager->page_size - sizeof(Free1SpaceHead))/ sizeof(Free1Entry);
}


int16_t Free1TableGetMaxFreeCount(FreeTable* free_table, Free1Table* free1_table) {
	return Free1SpaceManagerGetMaxFreeBlockSize(free1_table, kFree1ListFree);
}

//bool Free1TableIsPending(FreeTable* free_table, Free1Table* free1_table) {
//	//int16_t free1_entry_count = 0;
//	//int16_t free1_entry_max_count = 0;
//	//for (int16_t i = 0; i < Free1TableGetMaxCount(free_table); i++) {
//	//	if (free1_table[i].status != kFree1Free && free1_table[i].status != kFree1Alloc) {
//	//		return true;
//	//	}
//	//}
//	//return false;
//}

void Free1TableMarkDirty(FreeTable* table, Free1Table* free1_table) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	CacheId cache_id = CacherGetIdByBuf(&pager->cacher, free1_table);
	CacheInfo* cache_info = CacherGetInfo(&pager->cacher, cache_id);
	CacherMarkDirty(&pager->cacher, cache_id);
}

void Free1TableInit(FreeTable* free_table, Free1Table* free1_table) {
	Free1SpaceManagerInit(&free1_table->space_head, Free1TableGetMaxCount(free_table));
}

int16_t Free1TableAlloc(FreeTable* free_table, Free1Table* free1_table, int16_t count) {
	return Free1SpaceManagerAlloc(&free1_table->space_head, kFree1ListFree, count);
}

void Free1TableFree(FreeTable* free_table, Free1Table* free1_table, int16_t free1_entry_pos, int16_t count) {
	return Free1SpaceManagerFree(&free1_table->space_head, kFree1ListFree, free1_entry_pos, count);
}

//void Free1TablePending(FreeTable* free_table, int16_t free0_entry_pos, Free1Entry* free1_table, int16_t free1_entry_pos, int16_t count, PageId old_first_pgid) {
//	for (int16_t i = 0; i < count - 1; i++) {
//		free1_table[free1_entry_pos + i].next_pending_pgid = FreeTablePosToPageId(free_table, free0_entry_pos, free1_entry_pos + i + 1);
//	}
//	if (old_first_pgid == kPageInvalidId) {
//		// 第一个first_pgid始终指向自己
//		free1_table[free1_entry_pos + count - 1].next_pending_pgid = FreeTablePosToPageId(free_table, free0_entry_pos, free1_entry_pos);
//	}
//	else {
//		free1_table[free1_entry_pos + count - 1].next_pending_pgid = old_first_pgid;
//	}
//}

Free1Table* Free1TableGet(FreeTable* free_table, int16_t free0_entry_pos, CacheId* cache_id) {
	Pager* pager = ObjectGetFromField(free_table, Pager, free_table);
	YuDb* db = ObjectGetFromField(pager, Pager, free_table);

	Free0Entry* free0_entry = &free_table->space_head.obj_arr[free0_entry_pos];
	PageId free1_table_pgid_start;
	if (free0_entry_pos == 0) {
		free1_table_pgid_start = kFree1TableStartId;		// meta  free_level0_list  free_level1_list
	}
	else {
		free1_table_pgid_start = free0_entry_pos * Free1TableGetMaxCount(free_table);
	}
	
	// 从一端读取，写入到另一端
	PageId free1_table_pgid_read = free1_table_pgid_start + free0_entry->free1_table_read_select;
	PageId free1_table_pgid_write = free1_table_pgid_start + free0_entry->free1_table_write_select;
	
	Free1Entry* read_cache, * write_cache;

	CacheId read_cache_id = CacherFind(&pager->cacher, free1_table_pgid_read, true);
	CacheId write_cache_id;
	if (free1_table_pgid_read != free1_table_pgid_write) {
		write_cache_id = CacherFind(&pager->cacher, free1_table_pgid_write, true);
	}
	else {
		write_cache_id = read_cache_id;
	}
	if (write_cache_id == kCacheInvalidId) {
		write_cache_id = CacherAlloc(&pager->cacher, free1_table_pgid_write);
	}
	write_cache = CacherGet(&pager->cacher, write_cache_id);
	if (read_cache_id == kCacheInvalidId) {
		if (!PagerRead(pager, free1_table_pgid_read, write_cache, 1)) {
			// 如果读取失败，若是从未使用过的f1则将其初始化
			if (free0_entry->free1_table_max_free != Free1TableGetMaxCount(free_table)) {
				return NULL;
			}
			Free1TableInit(free_table, write_cache);
		}
	}
	else {
		if (free1_table_pgid_read != free1_table_pgid_write) {
			read_cache = (Free1Entry*)CacherGet(&pager->cacher, read_cache_id);
			memcpy(write_cache, read_cache, pager->page_size);
			CacherDereference(&pager->cacher, read_cache_id);
		}
	}

	if (free0_entry->free1_table_read_select != free0_entry->free1_table_write_select) {
		// 初次读之后，由于read的内容会被拷贝到write，并且write可能会被修改，故read需要置为与write同一端
		free0_entry->free1_table_read_select = free0_entry->free1_table_write_select;
	}
	if (cache_id) { *cache_id = write_cache_id; }
	return write_cache;
}


PageId FreeTablePosToPageId(FreeTable* free_table, int16_t free0_entry_pos, int16_t free1_entry_pos) {
	return free0_entry_pos * Free1TableGetMaxCount(free_table) + free1_entry_pos;
}

void FreeTableGetPosFromPageId(FreeTable* free_table, PageId pgid, int16_t* free0_entry_pos, int16_t* free1_entry_pos) {
	*free0_entry_pos = pgid / Free1TableGetMaxCount(free_table);
	*free1_entry_pos = pgid % Free1TableGetMaxCount(free_table);
}


bool FreeTableInit(FreeTable* table) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	YuDb* db = ObjectGetFromField(pager, YuDb, pager);

	// free0_table常驻内存
	Free0SpaceManagerInit(&table->space_head, Free0TableGetMaxCount(table));
	PageId free0_table_pgid = kFree0ListStartId + db->meta_index;
	if (!PagerRead(pager, free0_table_pgid, pager->free_table.free0_table.objArr, 1)) {
		return false;
	}
	return true;
}

int16_t FreeTableAlloc(FreeTable* table, int16_t count, int16_t* free0_entry_pos) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);

	int16_t free0_entry_pos_ = Free0SpaceManagerAlloc(&table->space_head, 0, 1);
	if (free0_entry_pos_ == -1) {
		return -1;
	}
	Free0Entry* free0_entry = &table->space_head.obj_arr[free0_entry_pos_];
	CacheId cache_id;
	Free1Entry* free1_table = Free1TableGet(table, free0_entry_pos_, &cache_id);
	int16_t free1_entry_pos = Free1TableAlloc(table, free1_table, count);
	if (free1_entry_pos == -1) {
		CacherDereference(&pager->cacher, cache_id);
		return -1;
	}
	free0_entry->free1_table_max_free = Free1TableGetMaxFreeCount(table, free1_table);
	free0_entry->free1_table_dirty = true;
	if (free0_entry_pos) {
		*free0_entry_pos = free0_entry_pos_;
	}
	Free1TableMarkDirty(table, free1_table);
	CacherDereference(&pager->cacher, cache_id);
	return free1_entry_pos;
}

void FreeTableFree(FreeTable* table, PageId pgid, int16_t count) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	int16_t free0_entry_pos;
	int16_t free1_entry_pos;
	FreeTableGetPosFromPageId(table, pgid, &free0_entry_pos, &free1_entry_pos);
	Free0Entry* free0_entry = &table->space_head.obj_arr[free0_entry_pos];
	CacheId cache_id;
	Free1Entry* free1_table = Free1TableGet(table, free0_entry_pos, &cache_id);
	Free1TableFree(table, free1_table, free1_entry_pos, count);
	free0_entry->free1_table_max_free = Free1TableGetMaxFreeCount(table, free1_table);
	free0_entry->free1_table_dirty = true;
	Free1TableMarkDirty(table, free1_table);
	CacherDereference(&pager->cacher, cache_id);
}

//void FreeTablePending(FreeTable* table, PageId pgid, int16_t count, PageId first_pgid) {
//	Pager* pager = ObjectGetFromField(table, Pager, free_table);
//	
//	int16_t free0_entry_pos;
//	int16_t free1_entry_pos;
//	FreeTableGetPosFromPageId(table, pgid, &free0_entry_pos, &free1_entry_pos);
//
//	CacheId cache_id;
//	Free1Entry* free1_table = Free1TableGet(table, free0_entry_pos, &cache_id);
//	Free1TablePending(table, free0_entry_pos, free1_table, free1_entry_pos, count, first_pgid);
//
//	Free0Entry* free0_entry = &table->space_head.obj_arr[free0_entry_pos];
//	free0_entry->free1_table_pending = 1;
//
//	free0_entry->free1_table_dirty = true;
//	Free1TableMarkDirty(table, free1_table);
//	CacherDereference(&pager->cacher, cache_id);
//}
//
//void FreeTableFreePending(FreeTable* table, PageId first_pgid) {
//	Pager* pager = ObjectGetFromField(table, Pager, free_table);
//
//	int16_t free0_entry_pos, free0_entry_old_pos = -1;
//	int16_t free1_entry_pos;
//	Free0Entry* free0_entry = NULL;
//	Free1Entry* free1_table = NULL;
//	CacheId cache_id = kCacheInvalidId;
//	do {
//		FreeTableGetPosFromPageId(table, first_pgid, &free0_entry_pos, &free1_entry_pos);
//		if (free0_entry_pos == -1) {
//			break;
//		}
//		if (free0_entry_pos != free0_entry_old_pos) {
//			if (free0_entry) {
//				free0_entry->free1_table_pending = Free1TableIsPending(table, free1_table);
//				free0_entry->free1_table_max_free = Free1TableGetMaxFreeCount(table, free1_table);
//				
//				BitmapSet(&table->free0_entry_dirty, free0_entry_pos, true);
//				Free1TableMarkDirty(table, free1_table);
//				CacherDereference(&pager->cacher, cache_id);
//			}
//			free0_entry = ArrayAt(&table->free0_table, free0_entry_pos, Free0Entry);
//			free1_table = Free1TableGet(table, free0_entry_pos, &cache_id);
//			free0_entry_old_pos = free0_entry_pos;
//		}
//		if (free1_table[free1_entry_pos].status == kFree1Free || free1_table[free1_entry_pos].status == kFree1Alloc) {
//			// 链表末尾页面始终指向自己，因此遇到Free就退出
//			break;
//		}
//		first_pgid = free1_table[free1_entry_pos].next_pending_pgid;
//		free1_table[free1_entry_pos].status = kFree1Free;
//	} while (true);
//	if (free0_entry) {
//		free0_entry->free1_table_pending = Free1TableIsPending(table, free1_table);
//		free0_entry->free1_table_max_free = Free1TableGetMaxFreeCount(table, free1_table);
//		BitmapSet(&table->free0_entry_dirty, free0_entry_pos, true);
//		Free1TableMarkDirty(table, free1_table);
//		CacherDereference(&pager->cacher, cache_id);
//	}
//}
//
//void FreeTableCleanPending(FreeTable* table) {
//	Pager* pager = ObjectGetFromField(table, Pager, free_table);
//	for (int16_t i = 0; i < Free0TableGetMaxCount(table); i++) {
//		Free0Entry* free0_entry = ArrayAt(&pager->free_table.free0_table, i, Free0Entry);
//		if (free0_entry->free1_table_pending == 0) {
//			continue;
//		}
//		CacheId cache_id;
//		Free1Entry* free1_table = Free1TableGet(table, i, &cache_id);
//		for (int16_t j = 0; j < Free1TableGetMaxCount(table); j++) {
//			if (free1_table[j].status == kFree1Alloc || free1_table[j].status == kFree1Free) {
//				continue;
//			}
//			free1_table[j].status = kFree1Free;
//		}
//		free0_entry->free1_table_max_free = Free1TableGetMaxFreeCount(table, free1_table);
//		free0_entry->free1_table_pending = 0;
//		BitmapSet(&table->free0_entry_dirty, i, true);
//		Free1TableMarkDirty(table, free1_table);
//		CacherDereference(&pager->cacher, cache_id);
//	}
//}

bool FreeTableWrite(FreeTable* table, int32_t meta_index) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	YuDb* db = ObjectGetFromField(pager, YuDb, pager);

	int64_t offset = ((int64_t)kFree0ListStartId + meta_index) * pager->page_size;
	ptrdiff_t pos = BitmapFindBit(&table->free0_entry_dirty, 0, true);
	if (pos == kBitmapInvalidIndex) {
		return true;
	}
	do {
		Free0Entry* entry = ArrayAt(&table->free0_table, pos, Free0Entry);

		// 落盘时read和write是相同的，write切到另一侧
		  assert(entry->free1_table_write_select == entry->free1_table_read_select);
		entry->free1_table_write_select = (entry->free1_table_read_select + 1) % 2;
		
		pos = BitmapFindBit(&table->free0_entry_dirty, pos + 1, true);
	} while (pos != kBitmapInvalidIndex);
	DbFileSeek(db->db_file, offset, kDbFilePointerSet);
	DbFileWrite(db->db_file, ArrayGetData(&table->free0_table), ArrayGetByteCount(&table->free0_table));
}