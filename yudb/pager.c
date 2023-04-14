#include <yudb/pager.h>

#include <yudb/db_file.h>
#include <yudb/free_table.h>
#include <yudb/yudb.h>

/*
* Page
*/

static int64_t PagerGetPageOffset(Pager* pager, PageId pgid) {
	return pager->page_size * pgid;
}

/*
* 初始化页面管理器
*/
bool PagerInit(Pager* pager, int16_t page_size, PageCount page_count, size_t cache_count) {
	YuDb* db = ObjectGetFromField(pager, YuDb, pager);
	pager->page_size = page_size;
	pager->page_count = page_count;
	CacherInit(&pager->cacher, cache_count);
	FreeTableInit(&pager->free_table);
	return true;
}

/*
* 从db文件中读取页面
*/
bool PagerRead(Pager* pager, PageId pgid, void* cache, PageCount count) {
	YuDb* db = ObjectGetFromField(pager, YuDb, pager);
	if (!DbFileSeek(db->db_file, PagerGetPageOffset(&db->pager, pgid), kDbFilePointerSet)) {
		return false;
	}
	if (!DbFileRead(db->db_file, cache, db->meta_info.page_size * count)) {
		return false;
	}
	return true;
}

/*
* 写回页面到db文件
*/
bool PagerWrite(Pager* pager, PageId pgid, void* cache, PageCount count) {
	YuDb* db = ObjectGetFromField(pager, YuDb, pager);
	if (!DbFileSeek(db->db_file, PagerGetPageOffset(&db->pager, pgid), kDbFilePointerSet)) {
		return false;
	}
	if (!DbFileWrite(db->db_file, cache, db->meta_info.page_size * count)) {
		return false;
	}
	return true;
}


// 只有写事务会调用页面分配、释放函数，所以无需加锁
/*
* 从db文件中分配页面
*/
PageId PagerAlloc(Pager* pager, bool put_cache, PageCount count){
	YuDb* db = ObjectGetFromField(pager, YuDb, pager);
	PageId disk_free_pgid;
	do {
		int16_t free0_entry_pos;
		int16_t free1_entry_pos = FreeTableAlloc(&pager->free_table, count, &free0_entry_pos);
		if (free1_entry_pos == -1) {
			return kPageInvalidId;
		}
		disk_free_pgid = FreeTablePosToPageId(&pager->free_table, free0_entry_pos, free1_entry_pos);
	} while (false);
	
	if (put_cache) {
		// 申请后可能就会使用，直接挂到缓存中可以避免在PageGet接口发生一次磁盘读取
		CacheId cache_id = CacherFind(&pager->cacher, disk_free_pgid, true);
		if (cache_id == kCacheInvalidId) {
			cache_id = CacherAlloc(&pager->cacher, disk_free_pgid);
			if (cache_id == kCacheInvalidId) {
				FreeTableFree(&pager->free_table, disk_free_pgid, count);
				return kPageInvalidId;
			}
		}
	}
	return disk_free_pgid;
}

/*
* 指定页面置为待决
*/
void PagerPending(Pager* pager, Tx* tx, PageId pgid) {
	TxRbEntry* entry = TxRbTreeFind(&tx->db->tx_manager.pending_page_list, &tx->meta_info.txid);
	  assert(entry != NULL);
	TxPendingListEntry* pending_list_entry = ObjectGetFromField(entry, TxPendingListEntry, rb_entry);
	TxVectorPushTail(&pending_list_entry->pending_pgid_arr, &pgid);
	FreeTablePending(&pager->free_table, pgid);
}

/*
* 释放分配的页面
* 如果引用计数未清零则挂到待释放链表中(或循环等待)等待引用计数清空
*/
void PagerFree(Pager* pager, PageId pgid) {
	CacheId cache_id = CacherFind(&pager->cacher, pgid, false);
	if (cache_id != kCacheInvalidId) {
		CacherFree(&pager->cacher, cache_id);
	}
	FreeTableFree(&pager->free_table, pgid);
}

/*
* 引用页面获取缓存，递增页面引用计数
*/
void* PagerReference(Pager* pager, PageId pgid) {
	CacheId cache_id = CacherFind(&pager->cacher, pgid, true);
	void* cache;
	if (cache_id == kCacheInvalidId) {
		cache_id = CacherAlloc(&pager->cacher, pgid);
		if (cache_id == kCacheInvalidId) {
			return NULL;
		}
		cache = CacherGet(&pager->cacher, cache_id);

		if (!PagerRead(pager, pgid, cache, 1)) {
			CacherFree(&pager->cacher, cache_id);
			// memset(cache, 0, pager->page_size);
			return NULL;
		}
	}
	else {
		cache = CacherGet(&pager->cacher, cache_id);
	}
	return cache;
}

/*
* 解除对指定页面的引用
*/
void PagerDereference(Pager* pager, void* cache) {
	CacheId cache_id = CacherGetIdByBuf(&pager->cacher, cache);
	if (cache_id != kCacheInvalidId) {
		CacherDereference(&pager->cacher, cache_id);
	}
}

/*
* 标记为脏页
*/
void PagerMarkDirty(Pager* pager, void* cache) {
	CacherMarkDirty(&pager->cacher, CacherGetIdByBuf(&pager->cacher, cache));
}

/*
* 所有脏页落盘
*/
void PagerWriteAllDirty(Pager* pager) {
	Cacher* cacher = &pager->cacher;
	YuDb* db = ObjectGetFromField(pager, YuDb, pager);
	// 尽可能顺序写
	do {
		CacheId dirty_cache_id = cacher->cache_info_pool->list_first[kCacheListDirty];
		if (dirty_cache_id == kCacheInvalidId) {
			break;
		}

		// 找最小页号的缓存
		CacheInfo* min_cache_info = NULL;
		do {
			CacheInfo* cache_info = CacherGetInfo(cacher, dirty_cache_id);
			  assert(cache_info->type == kCacheListDirty);
			  assert(cache_info->reference_count == 0);
			if (min_cache_info == NULL) {
				min_cache_info = cache_info;
			}
			else if (cache_info->pgid < min_cache_info->pgid) {
				min_cache_info = cache_info;
			}
			dirty_cache_id = cache_info->dirty_entry.next;
		} while (dirty_cache_id != kCacheInvalidId);

		// 写入1页
		if (min_cache_info) {
			dirty_cache_id = CacherGetIdByInfo(cacher, min_cache_info);
			void* cache = CacherGet(cacher, dirty_cache_id);
			CacheDoublyStaticListSwitch(cacher->cache_info_pool, kCacheListDirty, dirty_cache_id, kCacheListClean);
			min_cache_info->type = kCacheListClean;
			PagerWrite(pager, min_cache_info->pgid, cache, 1);
			CacherDereference(cacher, dirty_cache_id);
		}
	}  while (true);
	cacher->cache_info_pool->list_first[kCacheListDirty] = kCacheInvalidId;
}