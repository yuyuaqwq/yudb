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
CUTILS_CONTAINER_STATIC_LIST_DEFINE(Free0, int16_t, Free0Entry, YUDB_FREE_TABLE_FREE_REFERENCER, YUDB_FREE_TABLE_FREE0_ACCESSOR, 4)

CUTILS_CONTAINER_STATIC_LIST_DEFINE(Free1, int16_t, Free1Entry, YUDB_FREE_TABLE_FREE_REFERENCER, YUDB_FREE_TABLE_FREE0_ACCESSOR, 2)


const PageId kMetaStartId = 0;
const PageId kFree0ListStartId = 2;
const PageId kFree1TableStartId = 4;


static int16_t Free1TableGetMaxCount(int16_t page_size) {
	return (page_size - (page_size / 4)) / sizeof(Free1Entry);
}

static int16_t Free1GetPageSize(Free1Table* free1_table) {
	return FreeBuddyGetMaxCount(&free1_table->buddy) * 4;
}

Free1StaticList* Free1TableGetStaticList(Free1Table* free1_table) {
	return (Free1StaticList*)((uintptr_t)free1_table + FreeBuddyGetMaxCount(&free1_table->buddy));
}

int16_t Free1TableGetMaxFreeCount(Free1Table* free1_table) {
	return FreeBuddyGetMaxFreeCount(&free1_table->buddy);
}

void Free1TableInit(Free1Table* free1_table, int16_t page_size) {
	int16_t max_count = Free1TableGetMaxCount(page_size);
	FreeBuddyInit(&free1_table->buddy, max_count);
	max_count -= 2;
	Free1StaticListInit(Free1TableGetStaticList(free1_table), max_count);
}

int16_t Free1TableAlloc(Free1Table* free1_table, int16_t count) {
	return FreeBuddyAlloc(&free1_table->buddy, count);
}

void Free1TablePending(Free1Table* free1_table, int16_t free1_entry_id) {
	Free1StaticList* static_list = Free1TableGetStaticList(free1_table);
	Free1Entry* free1_entry = &static_list->obj_arr[free1_entry_id];
	free1_entry->is_pending = true;
	Free1StaticListPush(static_list, kFree1EntryListPending, free1_entry_id);
}

void Free1TableFree(Free1Table* free1_table, int16_t free1_entry_id) {
	Free1StaticList* static_list = Free1TableGetStaticList(free1_table);
	Free1Entry* free1_entry = &static_list->obj_arr[free1_entry_id];
	if (free1_entry->is_pending) {
		free1_entry->is_pending = false;
		// 将其从Pending链表中摘除
	}
	FreeBuddyFree(&free1_table->buddy, free1_entry_id);
}

void Free1TableMarkDirty(FreeTable* table, Free1Table* free1_table) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	CacheId cache_id = CacherGetIdByBuf(&pager->cacher, free1_table);
	CacheInfo* cache_info = CacherGetInfo(&pager->cacher, cache_id);
	CacherMarkDirty(&pager->cacher, cache_id);
}



static int16_t Free0TableGetMaxCount(int16_t page_size) {
	return (page_size - (page_size / 4)) / sizeof(Free0Entry);
}

static int16_t Free0GetPageSize(Free0Table* free0_table) {
	return FreeBuddyGetMaxCount(&free0_table->buddy) * 4;
}

Free0StaticList* Free0TableGetStaticList(Free0Table* free0_table) {
	return (Free0StaticList*)((uintptr_t)free0_table + FreeBuddyGetMaxCount(&free0_table->buddy));
}

void Free0TableInit(Free0Table* free0_table, int16_t page_size) {
	int16_t max_count = Free0TableGetMaxCount(page_size);
	FreeBuddyInit(&free0_table->buddy, max_count);
	max_count -= 2;
	Free0StaticList* static_list = Free0TableGetStaticList(free0_table);
	Free0StaticListInit(static_list, max_count);
	for (int i = 0; i < max_count; i++) {
		static_list->obj_arr[i].max_free_log = CUTILS_SPACE_MANAGER_BUDDY_TO_EXPONENT_OF_2(Free1TableGetMaxCount(page_size));
		static_list->obj_arr[i].read_select = 1;
		static_list->obj_arr[i].write_select = 0;
		static_list->obj_arr[i].free1_table_pending = false;
	}
	static_list->obj_arr[0].max_free_log -= 1;
}



Free1Table* Free1TableGet(FreeTable* free_table, int16_t free0_entry_pos, CacheId* cache_id) {
	Pager* pager = ObjectGetFromField(free_table, Pager, free_table);
	YuDb* db = ObjectGetFromField(pager, YuDb, pager);

	Free0Entry* free0_entry = &Free0TableGetStaticList(free_table->free0_table)->obj_arr[free0_entry_pos];
	PageId free1_table_pgid_start;
	if (free0_entry_pos == 0) {
		free1_table_pgid_start = kFree1TableStartId;		// meta  free_level0_list  free_level1_list
	}
	else {
		free1_table_pgid_start = free0_entry_pos * Free1TableGetMaxCount(Free0GetPageSize(free_table->free0_table));
	}
	
	// 从一端读取(最新版本)，写入到另一端(旧版本)
	PageId free1_table_pgid_read = free1_table_pgid_start + free0_entry->read_select;
	PageId free1_table_pgid_write = free1_table_pgid_start + free0_entry->write_select;
	
	Free1Entry* read_cache, * write_cache;
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
			if (CUTILS_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(free0_entry->max_free_log) != Free1TableGetMaxCount(Free0GetPageSize(free_table->free0_table))) {
				return NULL;
			}
			Free1TableInit(write_cache, pager->page_size);
		}
	}
	else {
		if (free1_table_pgid_read != free1_table_pgid_write) {
			read_cache = (Free1Entry*)CacherGet(&pager->cacher, read_cache_id);
			memcpy(write_cache, read_cache, pager->page_size);
			CacherDereference(&pager->cacher, read_cache_id);
		}
	}

	if (free0_entry->read_select != free0_entry->write_select) {
		// 初次读之后，由于read的内容会被拷贝到write，并且write可能会被修改，此时write才是最新的版本(尚未落盘)，下次read应该读取当前的write
		free0_entry->read_select = free0_entry->write_select;
	}
	if (cache_id) { *cache_id = write_cache_id; }
	return write_cache;
}

PageId FreeTablePosToPageId(FreeTable* free_table, int16_t free0_entry_pos, int16_t free1_entry_pos) {
	return free0_entry_pos * Free1TableGetMaxCount(Free0GetPageSize(free_table->free0_table)) + free1_entry_pos;
}

void FreeTableGetPosFromPageId(FreeTable* free_table, PageId pgid, int16_t* free0_entry_pos, int16_t* free1_entry_pos) {
	*free0_entry_pos = pgid / Free1TableGetMaxCount(Free0GetPageSize(free_table->free0_table));
	*free1_entry_pos = pgid % Free1TableGetMaxCount(Free0GetPageSize(free_table->free0_table));
}

/*
* 初始化空闲表
*/
bool FreeTableInit(FreeTable* free_table) {
	Pager* pager = ObjectGetFromField(free_table, Pager, free_table);
	YuDb* db = ObjectGetFromField(pager, YuDb, pager);

	int16_t max_count = Free0TableGetMaxCount(pager->page_size);
	free_table->free0_table = malloc(pager->page_size);

	// free0_table常驻内存
	PageId free0_table_pgid = kFree0ListStartId + db->meta_index;
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

	// 从f0中查找足够分配的f1
	int16_t free0_entry_prev_id = YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId;
	Free0StaticList* static_list = Free0TableGetStaticList(table->free0_table);
	int16_t free0_entry_id = Free0StaticListIteratorFirst(static_list, kFree0EntryListAlloc);
	while (true) {
		if (free0_entry_id == YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId) {
			free0_entry_id = FreeBuddyAlloc(&table->free0_table->buddy, 1);
			if (free0_entry_id == YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId) {
				return YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId;
			}
			Free0Entry* free0_entry = &static_list->obj_arr[free0_entry_id];
			Free0StaticListPush(static_list, kFree0EntryListAlloc, free0_entry_id);
			free0_entry->entry_list_type = kFree0EntryListAlloc;
		}
		int16_t max_free = CUTILS_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(static_list->obj_arr[free0_entry_id].max_free_log);
		if (max_free >= count) {
			break;
		}
		free0_entry_prev_id = free0_entry_id;
		free0_entry_id = Free0StaticListIteratorNext(static_list, free0_entry_id);
	}
	
	// 从f1分配页面
	Free0Entry* free0_entry = &static_list->obj_arr[free0_entry_id];
	CacheId cache_id;
	Free1Table* free1_table = Free1TableGet(table, free0_entry_id, &cache_id);
	  assert(free1_table != NULL);
	if (CUTILS_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(static_list->obj_arr[free0_entry_id].max_free_log) == Free1TableGetMaxCount(Free0GetPageSize(table->free0_table))) {
		// 初次分配的f1，前2页提前占用
		Free1TableAlloc(free1_table, 2);
	}
	int16_t free1_entry_id = Free1TableAlloc(free1_table, count);
	if (free1_entry_id == -1) {
		CacherDereference(&pager->cacher, cache_id);
		return -1;
	}

	// 更新f0中对应的f1最大连续空闲
	free0_entry->max_free_log = CUTILS_SPACE_MANAGER_BUDDY_TO_EXPONENT_OF_2(Free1TableGetMaxFreeCount(free1_table));
	if (free0_entry->max_free_log == 0) {
		// 下级没有可分配的空间，挂到满队列中
		Free0StaticListSwitch(static_list, kFree0EntryListAlloc, free0_entry_prev_id, free0_entry_id, kFree0EntryListFull);
	}

	// 该f1表已是脏页
	free0_entry->free1_table_dirty = true;
	if (free0_entry_id_out) {
		*free0_entry_id_out = free0_entry_id;
	}
	Free1TableMarkDirty(table, free1_table);

	CacherDereference(&pager->cacher, cache_id);
	return free1_entry_id;
}

/*
* 从空闲表中将页面置为待决状态
*/
void FreeTablePending(FreeTable* table, PageId pgid) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	Free0StaticList* static_list = Free0TableGetStaticList(table->free0_table);
	int16_t free0_entry_id;
	int16_t free1_entry_id;
	FreeTableGetPosFromPageId(table, pgid, &free0_entry_id, &free1_entry_id);

	CacheId cache_id;
	Free1Entry* free1_table = Free1TableGet(table, free0_entry_id, &cache_id);
	Free1TablePending(free1_table, free1_entry_id);
	Free1TableMarkDirty(table, free1_table);

	Free0Entry* free0_entry = &static_list->obj_arr[free0_entry_id];
	free0_entry->free1_table_dirty = true;
	free0_entry->free1_table_pending = true;

	CacherDereference(&pager->cacher, cache_id);
}

/*
* 从空闲表中释放页面
*/
void FreeTableFree(FreeTable* table, PageId pgid) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	Free0StaticList* static_list = Free0TableGetStaticList(table->free0_table);

	int16_t free0_entry_id;
	int16_t free1_entry_id;
	FreeTableGetPosFromPageId(table, pgid, &free0_entry_id, &free1_entry_id);
	Free0Entry* free0_entry = &static_list->obj_arr[free0_entry_id];

	CacheId cache_id;
	Free1Table* free1_table = Free1TableGet(table, free0_entry_id, &cache_id);
	  assert(free1_table != NULL);
	Free1TableFree(free1_table, free1_entry_id);
	Free1TableMarkDirty(table, free1_table);

	if (free0_entry->max_free_log == 0) {
		// 挂回可分配队列
		//Free0StaticListSwitch(static_list, kFree0EntryListFull, free0_entry_prev_id, free0_entry_id, kFree0EntryListAlloc);
	}
	free0_entry->max_free_log = CUTILS_SPACE_MANAGER_BUDDY_TO_EXPONENT_OF_2(Free1TableGetMaxFreeCount(free1_table));
	free0_entry->free1_table_dirty = true;

	CacherDereference(&pager->cacher, cache_id);
}

/*
* 将空闲表中所有的pending页面释放
*/
void FreeTableCleanPending(FreeTable* table) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	Free0StaticList* f0_static_list = Free0TableGetStaticList(table->free0_table);
	for (int16_t i = 0; i < Free1TableGetMaxCount(pager->page_size) - 2; i++) {
		Free0Entry* free0_entry = &f0_static_list->obj_arr[i];
		if (free0_entry->free1_table_pending == true) {
			CacheId cache_id;
			Free1Table* free1_table = Free1TableGet(table, i, &cache_id);
			  assert(free1_table != NULL);

			Free1StaticList* f1_static_list = Free1TableGetStaticList(free1_table);
			int16_t id = Free1StaticListIteratorFirst(f1_static_list, kFree1EntryListPending);
			while (id != YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId) {
				Free1TableFree(free1_table, id);
				id = Free1StaticListIteratorNext(f1_static_list, kFree1EntryListPending);
			}

			free0_entry->free1_table_pending = false;
			CacherDereference(&pager->cacher, cache_id);
		}
	}
}

/*
* 空闲表f0写入
*/
bool FreeTableWrite(FreeTable* table, int32_t meta_index) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	YuDb* db = ObjectGetFromField(pager, YuDb, pager);
	PageId pgid = ((int64_t)kFree0ListStartId + meta_index);

	bool dirty = false;
	Free0EntryListType list_type[] = { kFree0EntryListAlloc, kFree0EntryListFull };
	Free0StaticList* static_list = Free0TableGetStaticList(table->free0_table);
	for (int i = 0; i < sizeof(list_type) / sizeof(Free0EntryListType); i++) {
		int16_t free0_entry_id = Free0StaticListIteratorFirst(static_list, list_type[i]);
		while (free0_entry_id != YUDB_FREE_TABLE_FREE_REFERENCER_InvalidId) {
			Free0Entry* free0_entry = &static_list->obj_arr[free0_entry_id];
			// 有f1脏页的话，f0需要更新对应的select
			if (free0_entry->free1_table_dirty == true) {
				// 落盘时read和write是相同的，write切到另一侧
				  assert(free0_entry->write_select == free0_entry->read_select);
				free0_entry->write_select = (free0_entry->read_select + 1) % 2;
				free0_entry->free1_table_dirty = false;
				dirty = true;
			}
			free0_entry_id = Free0StaticListIteratorNext(static_list, free0_entry_id);
		}
	}
	if (dirty) {
		PagerWrite(pager, pgid, table->free0_table, 1);
	}
}