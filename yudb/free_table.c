#include "yudb/free_table.h"

#include "yudb/db_file.h"
#include "yudb/pager.h"
#include "yudb/yudb.h"

const PageId kMetaStartId = 0;
const PageId kFree0ListStartId = 2;
const PageId kFree1TableStartId = 4;

// Pending仍有优化空间
// 如free1_table头部添加字段next_pending_table，不同free1_table的pending通过这个连接
// 节省空间则entry使用位图可以从4字节降到2位
// 提高性能则内部通过添加字段pending_first，静态连接当前free1_table的所有pending_entry

typedef enum _Free1EntryStatus {
	kFree1Free = 0,
	kFree1Alloc = -1,
	// kFree1Pending,
} Free1EntryStatus;

typedef struct _Free1Entry {
	union {
		Free1EntryStatus status;
		PageId next_pending_pgid;
	};
} Free1Entry;


static int16_t Free0TableGetMaxCount(FreeTable* free_table) {
	Pager* pager = ObjectGetFromField(free_table, Pager, free_table);
	return pager->page_size / sizeof(Free0Entry);
}


static int16_t Free1TableGetMaxCount(FreeTable* free_table) {
	Pager* pager = ObjectGetFromField(free_table, Pager, free_table);
	return pager->page_size / sizeof(Free1Entry);
}


int16_t Free1TableGetMaxFreeCount(FreeTable* free_table, Free1Entry* free1_table) {
	int16_t free1_entry_count = 0;
	int16_t free1_entry_max_count = 0;
	for (int16_t i = 0; i < Free1TableGetMaxCount(free_table); i++) {
		if (free1_table[i].status == kFree1Free) {
			free1_entry_count++;
		}
		else {
			if (free1_entry_count > free1_entry_max_count) {
				free1_entry_max_count = free1_entry_count;
			}
			free1_entry_count = 0;
		}
	}
	if (free1_entry_count > free1_entry_max_count) {
		free1_entry_max_count = free1_entry_count;
	}
	return free1_entry_max_count;
}

bool Free1TableIsPending(FreeTable* free_table, Free1Entry* free1_table) {
	int16_t free1_entry_count = 0;
	int16_t free1_entry_max_count = 0;
	for (int16_t i = 0; i < Free1TableGetMaxCount(free_table); i++) {
		if (free1_table[i].status != kFree1Free && free1_table[i].status != kFree1Alloc) {
			return true;
		}
	}
	return false;
}


void Free1TableMarkDirty(FreeTable* table, Free1Entry* free1_table) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	CacheId cache_id = CacherGetIdByBuf(&pager->cacher, free1_table);
	CacheInfo* cache_info = CacherGetInfo(&pager->cacher, cache_id);
	CacherMarkDirty(&pager->cacher, cache_id);
}

void Free1TableInit(FreeTable* free_table, Free1Entry* free1_table) {
	// 前两个page已被free1_table0和free1_table1占用
	free1_table[0].status = kFree1Alloc;
	free1_table[1].status = kFree1Alloc;
	for (int16_t i = 2; i < Free1TableGetMaxCount(free_table); i++) {
		free1_table[i].status = kFree1Free;
	}
}


int16_t Free1TableAlloc(FreeTable* free_table, Free1Entry* free1_table, int16_t count) {
	int16_t free1_entry_pos = 0;
	int16_t free1_entry_count = 0;
	for (int16_t i = 0; i < Free1TableGetMaxCount(free_table); i++) {
		if (free1_table[i].status == kFree1Free) {
			if (free1_entry_pos == 0) {
				free1_entry_pos = i;
			}
			free1_entry_count++;
			if (free1_entry_count >= count) {
				for (int16_t i = 0; i < free1_entry_count; i++) {
					free1_table[i + free1_entry_pos].status = kFree1Alloc;
				}
				return free1_entry_pos;
			}
		}
		else {
			free1_entry_pos = 0;
			free1_entry_count = 0;
		}
	}
	return -1;
}

void Free1TableFree(FreeTable* free_table, Free1Entry* free1_table, int16_t free1_entry_pos, int16_t count) {
	for (int16_t i = 0; i < count; i++) {
		free1_table[free1_entry_pos + i].status = kFree1Free;
	}
}

void Free1TablePending(FreeTable* free_table, int16_t free0_entry_pos, Free1Entry* free1_table, int16_t free1_entry_pos, int16_t count, PageId old_first_pgid) {
	for (int16_t i = 0; i < count - 1; i++) {
		free1_table[free1_entry_pos + i].next_pending_pgid = FreeTablePosToPageId(free_table, free0_entry_pos, free1_entry_pos + i + 1);
	}
	if (old_first_pgid == kPageInvalidId) {
		// 第一个first_pgid始终指向自己
		free1_table[free1_entry_pos + count - 1].next_pending_pgid = FreeTablePosToPageId(free_table, free0_entry_pos, free1_entry_pos);
	}
	else {
		free1_table[free1_entry_pos + count - 1].next_pending_pgid = old_first_pgid;
	}
}

Free1Entry* Free1TableGet(FreeTable* free_table, int16_t free0_entry_pos, CacheId* cache_id) {
	Free0Entry* free0_entry = ArrayAt(&free_table->free0_table, free0_entry_pos, Free0Entry);
	int16_t free1_table_select = free0_entry->free1_table_select;

	PageId free1_table_pgid_start;
	if (free0_entry_pos == 0) {
		free1_table_pgid_start = kFree1TableStartId;		// meta  free_level0_list  free_level1_list
	}
	else {
		free1_table_pgid_start = free0_entry_pos * Free1TableGetMaxCount(free_table);
	}
	// 读取从有效的一端读取，写入则写到另一侧
	PageId free1_table_pgid_read = free1_table_pgid_start + free1_table_select;
	PageId free1_table_pgid_write = free1_table_pgid_start + ((free1_table_select + 1) % 2);

	Pager* pager = ObjectGetFromField(free_table, Pager, free_table);

	Free1Entry* read_cache, * write_cache;
	CacheId write_cache_id = CacherFind(&pager->cacher, free1_table_pgid_write, true);
	
	// write_cache如果是脏缓存，则返回，否则从read中拷贝
	if (write_cache_id != kCacheInvalidId) {
		if (cache_id) { *cache_id = write_cache_id; }
		write_cache = (Free1Entry*)CacherGet(&pager->cacher, write_cache_id);
		CacheInfo* write_cache_info = CacherGetInfo(&pager->cacher, write_cache_id);
		if (write_cache_info->type == kCacheListDirty) {
			return write_cache;
		}
	}
	else {
		write_cache_id = CacherAlloc(&pager->cacher, free1_table_pgid_write);
		if (write_cache_id == kCacheInvalidId) {
			return NULL;
		}
		if (cache_id) { *cache_id = write_cache_id; }
		write_cache = (Free1Entry*)CacherGet(&pager->cacher, write_cache_id);
	}
	CacheId read_cache_id = CacherFind(&pager->cacher, free1_table_pgid_read, true);
	if (read_cache_id == kCacheInvalidId) {
		if (!PagerRead(pager, free1_table_pgid_read, write_cache, 1)) {
			// 如果读取失败，若是从未使用过的f1则将其初始化
			if (free0_entry->free1_table_max_free != Free1TableGetMaxCount(free_table)) {
				CacherDereference(&pager->cacher, write_cache_id);
				return NULL;
			}
			Free1TableInit(free_table, write_cache);
		}
		return write_cache;
	}
	read_cache = (Free1Entry*)CacherGet(&pager->cacher, read_cache_id);
	memcpy(write_cache, read_cache, pager->page_size);
	CacherDereference(&pager->cacher, read_cache_id);
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
	ArrayInit(&table->free0_table, pager->page_size / sizeof(Free0Entry), sizeof(Free0Entry));
	ArraySetCount(&pager->free_table.free0_table, pager->page_size / sizeof(Free0Entry));
	PageId free0_table_pgid = 2 + (db->meta_info.txid % 2);
	if (!PagerRead(pager, free0_table_pgid, pager->free_table.free0_table.objArr, 1)) {
		ArrayRelease(&pager->free_table.free0_table);
		return false;
	}
	BitmapInit(&table->free0_entry_dirty, pager->page_size / sizeof(Free0Entry));
	return true;
}

int16_t FreeTableAlloc(FreeTable* table, int16_t count, int16_t* free0_entry_pos) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);

	int16_t free0_entry_pos_ = -1;
	Free0Entry* free0_entry = NULL;
	for (int16_t i = 0; i < ArrayGetCount(&table->free0_table); i++) {
		free0_entry = ArrayAt(&table->free0_table, i, Free0Entry);
		if (free0_entry->free1_table_max_free >= count) {
			free0_entry_pos_ = i;		// 从level0中找到空位足够的level1
			break;
		}
	}
	if (free0_entry_pos_ == -1) {
		return -1;
	}
	CacheId cache_id;
	Free1Entry* free1_table = Free1TableGet(table, free0_entry_pos_, &cache_id);
	int16_t free1_entry_pos = Free1TableAlloc(table, free1_table, count);
	if (free1_entry_pos == -1) {
		CacherDereference(&pager->cacher, cache_id);
		return -1;
	}
	free0_entry->free1_table_max_free -= count;
	BitmapSet(&table->free0_entry_dirty, free0_entry_pos_, true);
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

	Free0Entry* free0_entry = ArrayAt(&table->free0_table, free0_entry_pos, Free0Entry);
	CacheId cache_id;
	Free1Entry* free1_table = Free1TableGet(table, free0_entry_pos, &cache_id);
	Free1TableFree(table, free1_table, free1_entry_pos, count);
	free0_entry->free1_table_max_free += count;
	BitmapSet(&table->free0_entry_dirty, free0_entry_pos, true);
	Free1TableMarkDirty(table, free1_table);
	CacherDereference(&pager->cacher, cache_id);
}

void FreeTablePending(FreeTable* table, PageId pgid, int16_t count, PageId first_pgid) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	
	int16_t free0_entry_pos;
	int16_t free1_entry_pos;
	FreeTableGetPosFromPageId(table, pgid, &free0_entry_pos, &free1_entry_pos);

	CacheId cache_id;
	Free1Entry* free1_table = Free1TableGet(table, free0_entry_pos, &cache_id);
	Free1TablePending(table, free0_entry_pos, free1_table, free1_entry_pos, count, first_pgid);

	Free0Entry* free0_entry = ArrayAt(&table->free0_table, free0_entry_pos, Free0Entry);
	free0_entry->free1_table_pending = 1;

	BitmapSet(&table->free0_entry_dirty, free0_entry_pos, true);
	Free1TableMarkDirty(table, free1_table);
	CacherDereference(&pager->cacher, cache_id);
}

void FreeTableFreePending(FreeTable* table, PageId first_pgid) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);

	int16_t free0_entry_pos, free0_entry_old_pos = -1;
	int16_t free1_entry_pos;
	Free0Entry* free0_entry = NULL;
	Free1Entry* free1_table = NULL;
	CacheId cache_id = kCacheInvalidId;
	do {
		FreeTableGetPosFromPageId(table, first_pgid, &free0_entry_pos, &free1_entry_pos);
		if (free0_entry_pos == -1) {
			break;
		}
		if (free0_entry_pos != free0_entry_old_pos) {
			if (free0_entry) {
				free0_entry->free1_table_pending = Free1TableIsPending(table, free1_table);
				free0_entry->free1_table_max_free = Free1TableGetMaxFreeCount(table, free1_table);
				
				BitmapSet(&table->free0_entry_dirty, free0_entry_pos, true);
				Free1TableMarkDirty(table, free1_table);
				CacherDereference(&pager->cacher, cache_id);
			}
			free0_entry = ArrayAt(&table->free0_table, free0_entry_pos, Free0Entry);
			free1_table = Free1TableGet(table, free0_entry_pos, &cache_id);
			free0_entry_old_pos = free0_entry_pos;
		}
		if (free1_table[free1_entry_pos].status == kFree1Free || free1_table[free1_entry_pos].status == kFree1Alloc) {
			// 链表末尾页面始终指向自己，因此遇到Free就退出
			break;
		}
		first_pgid = free1_table[free1_entry_pos].next_pending_pgid;
		free1_table[free1_entry_pos].status = kFree1Free;
	} while (true);
	if (free0_entry) {
		free0_entry->free1_table_pending = Free1TableIsPending(table, free1_table);
		free0_entry->free1_table_max_free = Free1TableGetMaxFreeCount(table, free1_table);
		BitmapSet(&table->free0_entry_dirty, free0_entry_pos, true);
		Free1TableMarkDirty(table, free1_table);
		CacherDereference(&pager->cacher, cache_id);
	}
}

void FreeTableCleanPending(FreeTable* table) {
	Pager* pager = ObjectGetFromField(table, Pager, free_table);
	for (int16_t i = 0; i < Free0TableGetMaxCount(table); i++) {
		Free0Entry* free0_entry = ArrayAt(&pager->free_table.free0_table, i, Free0Entry);
		if (free0_entry->free1_table_pending == 0) {
			continue;
		}
		CacheId cache_id;
		Free1Entry* free1_table = Free1TableGet(table, i, &cache_id);
		for (int16_t j = 0; j < Free1TableGetMaxCount(table); j++) {
			if (free1_table[j].status == kFree1Alloc || free1_table[j].status == kFree1Free) {
				continue;
			}
			free1_table[j].status = kFree1Free;
		}
		free0_entry->free1_table_max_free = Free1TableGetMaxFreeCount(table, free1_table);
		free0_entry->free1_table_pending = 0;
		BitmapSet(&table->free0_entry_dirty, i, true);
		Free1TableMarkDirty(table, free1_table);
		CacherDereference(&pager->cacher, cache_id);
	}
}

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
		entry->free1_table_select = (entry->free1_table_select + 1) % 2;
		pos = BitmapFindBit(&table->free0_entry_dirty, pos + 1, true);
	} while (pos != kBitmapInvalidIndex);
	DbFileSeek(db->db_file, offset, kDbFilePointerSet);
	DbFileWrite(db->db_file, ArrayGetData(&table->free0_table), ArrayGetByteCount(&table->free0_table));
}