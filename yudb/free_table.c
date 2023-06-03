#include "yudb/free_table.h"

#include "yudb/db_file.h"
#include "yudb/pager.h"
#include "yudb/yudb.h"


CUTILS_SPACE_MANAGER_BUDDY_DEFINE(Free, int16_t, CUTILS_SPACE_MANAGER_BUDDY_4BIT_INDEXER, CUTILS_OBJECT_ALLOCATOR_DEFALUT)


#define YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId (-1)
#define YUDB_FREE_TABLE_FREE_REFERENCER YUDB_FREE_TABLE_FREE_REFERENCER
#define YUDB_FREE_TABLE_FREE0_ACCESSOR_GetNext(list, element) ((element)->entry_list_next)
#define YUDB_FREE_TABLE_FREE0_ACCESSOR_SetNext(list, element, new_next) ((element)->entry_list_next = new_next)
#define YUDB_FREE_TABLE_FREE0_ACCESSOR YUDB_FREE_TABLE_FREE0_ACCESSOR
CUTILS_CONTAINER_STATIC_LIST_DEFINE(FreeDir, int16_t, FreeDirEntry, YUDB_FREE_TABLE_FREE_REFERENCER, YUDB_FREE_TABLE_FREE0_ACCESSOR, 4)

CUTILS_CONTAINER_STATIC_LIST_DEFINE(FreePage, int16_t, FreePageEntry, YUDB_FREE_TABLE_FREE_REFERENCER, YUDB_FREE_TABLE_FREE0_ACCESSOR, 2)


const PageId kMetaStartId = 0;
const PageId kFreeDirTableStartId = 2;
const PageId kFreePageTableStartId = 4;

const uint16_t kFreePageStaticEntryIdOffset = 2;


/*
* FreePageTable
*/
static int16_t FreePageTableGetMaxCount(int16_t page_size) {
	return (page_size - (page_size / 4)) / sizeof(FreePageEntry);
}

static int16_t FreePageGetPageSize(FreePageTable* free1_table) {
	return FreeBuddyGetMaxCount(&free1_table->buddy) * 4;
}


FreePageStaticList* FreePageTableGetStaticList(FreePageTable* free1_table) {
	return (FreePageStaticList*)((uintptr_t)free1_table + FreeBuddyGetMaxCount(&free1_table->buddy));
}

int16_t FreePageTableGetMaxFreeCount(FreePageTable* free1_table) {
	return FreeBuddyGetMaxFreeCount(&free1_table->buddy);
}

void FreePageTableInit(FreePageTable* free1_table, int16_t page_size) {
	int16_t max_count = FreePageTableGetMaxCount(page_size);
	FreeBuddyInit(&free1_table->buddy, max_count);
	max_count -= kFreePageStaticEntryIdOffset;
	FreePageStaticListInit(FreePageTableGetStaticList(free1_table), max_count);
}

int16_t FreePageTableAlloc(FreePageTable* free1_table, int16_t count) {
	return FreeBuddyAlloc(&free1_table->buddy, count);
}

void FreePageTablePending(FreePageTable* free1_table, int16_t free1_entry_id) {
	  assert(free1_entry_id != -1);
	FreePageStaticList* static_list = FreePageTableGetStaticList(free1_table);
	FreePageEntry* free1_entry = &static_list->obj_arr[free1_entry_id - kFreePageStaticEntryIdOffset];
	free1_entry->is_pending = true;
	FreePageStaticListPush(static_list, kFreePageEntryListPending, free1_entry_id - kFreePageStaticEntryIdOffset);
}

void FreePageTableFree(FreePageTable* free1_table, int16_t free1_entry_id) {
	  assert(free1_entry_id != -1);
	FreePageStaticList* static_list = FreePageTableGetStaticList(free1_table);
	FreePageEntry* free1_entry = &static_list->obj_arr[free1_entry_id - kFreePageStaticEntryIdOffset];
	if (free1_entry->is_pending) {
		free1_entry->is_pending = false;
		// 将其从Pending链表中摘除
		int16_t cur_id = FreePageStaticListIteratorFirst(static_list, kFreePageEntryListPending);
		int16_t prev_id = YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId;
		while (cur_id != YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId) {
			if (cur_id == free1_entry_id - kFreePageStaticEntryIdOffset) {
				FreePageStaticListDelete(static_list, kFreePageEntryListPending, prev_id, cur_id);
				break;
			}
			prev_id = cur_id;
			cur_id = FreePageStaticListIteratorNext(static_list, cur_id);
		}
	}
	FreeBuddyFree(&free1_table->buddy, free1_entry_id);
}

void FreePageTableMarkDirty(FreeTable* table, FreePageTable* free1_table) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	CacheId cache_id = CacherGetIdByBuf(&pager->cacher, free1_table);
	CacheInfo* cache_info = CacherGetInfo(&pager->cacher, cache_id);
	CacherMarkDirty(&pager->cacher, cache_id);
}


/*
* FreeDirTable
*/
static int16_t FreeDirTableGetMaxCount(int16_t page_size) {
	return (page_size - (page_size / 4)) / sizeof(FreeDirEntry);
}

static int16_t FreeDirGetPageSize(FreeDirTable* free0_table) {
	return FreeBuddyGetMaxCount(&free0_table->buddy) * 4;
}


FreeDirStaticList* FreeDirTableGetStaticList(FreeDirTable* free0_table) {
	return (FreeDirStaticList*)((uintptr_t)free0_table + FreeBuddyGetMaxCount(&free0_table->buddy));
}

void FreeDirTableInit(FreeDirTable* free0_table, int16_t page_size, FreeTableType sub_table_type) {
	int16_t max_count = FreeDirTableGetMaxCount(page_size);
	FreeBuddyInit(&free0_table->buddy, max_count);
	max_count -= 3;
	FreeDirStaticList* static_list = FreeDirTableGetStaticList(free0_table);
	FreeDirStaticListInit(static_list, max_count);
	for (int i = 0; i < max_count; i++) {
		static_list->obj_arr[i].sub_max_free_log = FreeBuddyToExponentOf2(sub_table_type == kFreeDirTable ? FreePageTableGetMaxCount(page_size) : FreeDirTableGetMaxCount(page_size)) + 1;
		static_list->obj_arr[i].read_select = 1;
		static_list->obj_arr[i].write_select = 0;
		static_list->obj_arr[i].sub_table_pending = false;
	}
	static_list->obj_arr[0].sub_max_free_log -= 1;
}

int16_t FreeDirTableAlloc(FreeDirTable* dir_table, int16_t count) {
	return FreeBuddyAlloc(&dir_table->buddy, count);
}

int16_t FreeDirTableFree(FreeDirTable* dir_table, int16_t free0_entry_id) {

}

int16_t FreeDirTableFindBySubFreeCount(FreeDirTable* dir_table, int16_t sub_count) {
	int16_t free0_entry_prev_id = YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId;
	FreeDirStaticList* static_list = FreeDirTableGetStaticList(dir_table);
	int16_t free0_entry_id = FreeDirStaticListIteratorFirst(static_list, kFreeDirEntryListAlloc);
	while (true) {
		if (free0_entry_id == YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId) {
			free0_entry_id = FreeDirTableAlloc(dir_table, 1);
			if (free0_entry_id == YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId) {
				return YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId;
			}
			FreeDirEntry* free0_entry = &static_list->obj_arr[free0_entry_id];
			FreeDirStaticListPush(static_list, kFreeDirEntryListAlloc, free0_entry_id);
			free0_entry->entry_list_type = kFreeDirEntryListAlloc;
		}
		int16_t sub_max_free = CUTILS_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(static_list->obj_arr[free0_entry_id].sub_max_free_log-1);
		if (sub_max_free >= sub_count) {
			break;
		}
		free0_entry_prev_id = free0_entry_id;
		free0_entry_id = FreeDirStaticListIteratorNext(static_list, free0_entry_id);
	}
	return free0_entry_id;
}

void* FreeDirTableGetSubTable(FreeTable* free_table, int16_t free0_entry_id, CacheId* cache_id) {
	Pager* pager = ObjectGetFromField(free_table, Pager, free_table);
	YuDb* db = ObjectGetFromField(pager, YuDb, pager);

	FreeDirEntry* free0_entry = &FreeDirTableGetStaticList(free_table->free0_table)->obj_arr[free0_entry_id];
	PageId free1_table_pgid_start;
	if (free0_entry_id == 0) {
		free1_table_pgid_start = kFreePageTableStartId;		// meta  free_level0_list  free_level1_list
	}
	else {
		free1_table_pgid_start = free0_entry_id * FreePageTableGetMaxCount(FreeDirGetPageSize(free_table->free0_table));
	}
	
	// 从一端读取(最新版本)，写入到另一端(旧版本)
	PageId free1_table_pgid_read = free1_table_pgid_start + free0_entry->read_select;
	PageId free1_table_pgid_write = free1_table_pgid_start + free0_entry->write_select;
	
	FreePageEntry* read_cache, * write_cache;
	CacheId read_cache_id = CacherFind(&pager->cacher, free1_table_pgid_read, true), write_cache_id;
	if (free1_table_pgid_read != free1_table_pgid_write) {
		write_cache_id = CacherFind(&pager->cacher, free1_table_pgid_write, true);
	} else {
		write_cache_id = read_cache_id;
	}
	if (write_cache_id == kCacheInvalidId) {
		write_cache_id = CacherAlloc(&pager->cacher, free1_table_pgid_write);
	}
	write_cache = CacherGet(&pager->cacher, write_cache_id);

	if (read_cache_id == kCacheInvalidId) {
		if (!PagerRead(pager, free1_table_pgid_read, write_cache, 1)) {
			// 如果读取失败，若是从未使用过的f1则将其初始化
			if (CUTILS_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(free0_entry->sub_max_free_log-1) != FreePageTableGetMaxCount(FreeDirGetPageSize(free_table->free0_table))) {
				return NULL;
			}
			FreePageTableInit(write_cache, pager->page_size);
		}
	}
	else {
		if (free1_table_pgid_read != free1_table_pgid_write) {
			read_cache = (FreePageEntry*)CacherGet(&pager->cacher, read_cache_id);
			memcpy(write_cache, read_cache, pager->page_size);
			CacherDereference(&pager->cacher, read_cache_id);
		}
	}

	if (free0_entry->read_select != free0_entry->write_select) {
		// 初次从磁盘读取之后，由于read的内容会被拷贝到write，并且write可能会被修改，此时write才是最新的版本(尚未落盘)，下次read应该读取当前的write
		free0_entry->read_select = free0_entry->write_select;
	}
	if (cache_id) { *cache_id = write_cache_id; }
	return write_cache;
}


/*
* FreeTable
*/
PageId FreeTablePosToPageId(FreeTable* free_table, int16_t free0_entry_id, int16_t free1_entry_pos) {
	return free0_entry_id * FreePageTableGetMaxCount(FreeDirGetPageSize(free_table->free0_table)) + free1_entry_pos;
}

void FreeTableGetPosFromPageId(FreeTable* free_table, PageId pgid, int16_t* free0_entry_id, int16_t* free1_entry_id) {
	*free0_entry_id = pgid / FreePageTableGetMaxCount(FreeDirGetPageSize(free_table->free0_table));
	*free1_entry_id = pgid % FreePageTableGetMaxCount(FreeDirGetPageSize(free_table->free0_table));
}


/*
* 初始化空闲表
*/
bool FreeTableInit(FreeTable* free_table) {
	Pager* pager = ObjectGetFromField(free_table, Pager, free_table);
	YuDb* db = ObjectGetFromField(pager, YuDb, pager);

	int16_t max_count = FreeDirTableGetMaxCount(pager->page_size);
	free_table->free0_table = malloc(pager->page_size);

	// free0_table常驻内存
	PageId free0_table_pgid = kFreeDirTableStartId + db->meta_index;
	if (!PagerRead(pager, free0_table_pgid, db->pager.free_table.free0_table, 1)) {
		return false;
	}
	return true;
}

/*
* 从空闲表中分配页面，返回f1_id
*/
int16_t FreeTableAlloc(FreeTable* table, int16_t count, int16_t* free0_entry_id_out) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);

	// 从dir_table中查找足够空位的sub_table
	int16_t free0_entry_id = FreeDirTableFindBySubFreeCount(table->free0_table, count);
	FreeDirStaticList* static_list = FreeDirTableGetStaticList(table->free0_table);
	FreeDirEntry* free0_entry = &static_list->obj_arr[free0_entry_id];

	// 从f1分配页面
	CacheId cache_id;
	FreePageTable* free1_table = FreeDirTableGetSubTable(table, free0_entry_id, &cache_id);
	  assert(free1_table != NULL);
	if (CUTILS_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(static_list->obj_arr[free0_entry_id].sub_max_free_log-1) == FreePageTableGetMaxCount(FreeDirGetPageSize(table->free0_table))) {
		// 初次分配的f1，前2页提前占用
		FreePageTableAlloc(free1_table, kFreePageStaticEntryIdOffset);
	}
	int16_t free1_entry_id = FreePageTableAlloc(free1_table, count);
	if (free1_entry_id == -1) {
		CacherDereference(&pager->cacher, cache_id);
		return -1;
	}

	// 更新f0中对应的f1最大连续空闲
	free0_entry->sub_max_free_log = FreeBuddyToExponentOf2(FreePageTableGetMaxFreeCount(free1_table)) + 1;
	if (free0_entry->sub_max_free_log == 0) {
		// 下级没有可分配的空间，挂到满队列中
		// FreeDirStaticListSwitch(static_list, kFreeDirEntryListAlloc, free0_entry_prev_id, free0_entry_id, kFreeDirEntryListFull);
	}

	// 该f1表已是脏页
	free0_entry->sub_table_dirty = true;
	if (free0_entry_id_out) {
		*free0_entry_id_out = free0_entry_id;
	}
	FreePageTableMarkDirty(table, free1_table);

	CacherDereference(&pager->cacher, cache_id);
	  assert(free1_entry_id);
	return free1_entry_id;
}

/*
* 从空闲表中将页面置为待决状态
*/
void FreeTablePending(FreeTable* table, PageId pgid) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	FreeDirStaticList* static_list = FreeDirTableGetStaticList(table->free0_table);
	int16_t free0_entry_id;
	int16_t free1_entry_id;
	FreeTableGetPosFromPageId(table, pgid, &free0_entry_id, &free1_entry_id);

	CacheId cache_id;
	FreePageEntry* free1_table = FreeDirTableGetSubTable(table, free0_entry_id, &cache_id);
	FreePageTablePending(free1_table, free1_entry_id);
	FreePageTableMarkDirty(table, free1_table);

	FreeDirEntry* free0_entry = &static_list->obj_arr[free0_entry_id];
	free0_entry->sub_table_dirty = true;
	free0_entry->sub_table_pending = true;

	CacherDereference(&pager->cacher, cache_id);
}

/*
* 从空闲表中释放页面
*/
void FreeTableFree(FreeTable* table, PageId pgid) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	FreeDirStaticList* static_list = FreeDirTableGetStaticList(table->free0_table);

	int16_t free0_entry_id;
	int16_t free1_entry_id;
	FreeTableGetPosFromPageId(table, pgid, &free0_entry_id, &free1_entry_id);
	FreeDirEntry* free0_entry = &static_list->obj_arr[free0_entry_id];

	CacheId cache_id;
	FreePageTable* free1_table = FreeDirTableGetSubTable(table, free0_entry_id, &cache_id);
	  assert(free1_table != NULL);
	FreePageTableFree(free1_table, free1_entry_id);
	FreePageTableMarkDirty(table, free1_table);

	if (free0_entry->sub_max_free_log == 0) {
		// 挂回可分配队列
		//FreeDirStaticListSwitch(static_list, kFreeDirEntryListFull, free0_entry_prev_id, free0_entry_id, kFreeDirEntryListAlloc);
	}
	free0_entry->sub_max_free_log = FreeBuddyToExponentOf2(FreePageTableGetMaxFreeCount(free1_table))+1;
	free0_entry->sub_table_dirty = true;

	CacherDereference(&pager->cacher, cache_id);
}

/*
* 将空闲表中所有的pending页面释放
*/
void FreeTableCleanPending(FreeTable* table) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	FreeDirStaticList* f0_static_list = FreeDirTableGetStaticList(table->free0_table);
	for (int16_t i = 0; i < FreePageTableGetMaxCount(pager->page_size) - 3; i++) {
		FreeDirEntry* free0_entry = &f0_static_list->obj_arr[i];
		// 遍历f0_entry，将存在pending的f1清空的pending页面释放
		if (free0_entry->sub_table_pending == true) {
			CacheId cache_id;
			FreePageTable* free1_table = FreeDirTableGetSubTable(table, i, &cache_id);
			  assert(free1_table != NULL);

			FreePageStaticList* f1_static_list = FreePageTableGetStaticList(free1_table);
			int16_t id = FreePageStaticListIteratorFirst(f1_static_list, kFreePageEntryListPending);
			while (id != YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId) {
				FreePageTableFree(free1_table, id + kFreePageStaticEntryIdOffset);
				id = FreePageStaticListIteratorNext(f1_static_list, id);
			}
			f1_static_list->list_first[kFreePageEntryListPending] = YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId;

			free0_entry->sub_table_pending = false;
			CacherDereference(&pager->cacher, cache_id);
		}
	}
}

/*
* 空闲表写入
*/
bool FreeTableWrite(FreeTable* table, int32_t meta_index) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	YuDb* db = ObjectGetFromField(pager, YuDb, pager);
	PageId pgid = ((int64_t)kFreeDirTableStartId + meta_index);

	bool dirty = false;
	FreeDirEntryListType list_type[] = { kFreeDirEntryListAlloc, /*kFreeDirEntryListFull*/ };
	FreeDirStaticList* static_list = FreeDirTableGetStaticList(table->free0_table);
	for (int i = 0; i < sizeof(list_type) / sizeof(FreeDirEntryListType); i++) {
		int16_t free0_entry_id = FreeDirStaticListIteratorFirst(static_list, list_type[i]);
		while (free0_entry_id != YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId) {
			FreeDirEntry* free0_entry = &static_list->obj_arr[free0_entry_id];
			// 有f1脏页的话，f0需要更新对应的select
			if (free0_entry->sub_table_dirty == true) {
				// 落盘时read和write是相同的，write切到另一侧
				  assert(free0_entry->write_select == free0_entry->read_select);
				free0_entry->write_select = (free0_entry->read_select + 1) % 2;
				free0_entry->sub_table_dirty = false;
				dirty = true;
			}
			free0_entry_id = FreeDirStaticListIteratorNext(static_list, free0_entry_id);
		}
	}
	if (dirty) {
		return PagerWrite(pager, pgid, table->free0_table, 1);
	}
	return true;
}