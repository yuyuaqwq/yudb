#include <yudb/cacher.h>

#include <libyuc/algorithm/hash_code.h>
#include <libyuc/concurrency/thread.h>
#include <libyuc/concurrency/atomic.h>

#include <yudb/pager.h>
#include <yudb/yudb.h>

const CacheId kCacheInvalidId = -1;


#define YUDB_CACHER_LRU_LIST_ACCESSOR_GetKey(LIST, ENTRY) (&ObjectGetFromField(ENTRY, CacheInfo, lru_entry)->pgid)
#define YUDB_CACHER_LRU_LIST_ACCESSOR_SetKey(LIST, ENTRY, PGID) (ObjectGetFromField(ENTRY, CacheInfo, lru_entry)->pgid = *(PGID))
#define YUDB_CACHER_LRU_LIST_ACCESSOR YUDB_CACHER_LRU_LIST_ACCESSOR
#define YUDB_CAHCER_LRU_LIST_HASHER(TABLE, KEY) HashCode_hashint(*KEY)
LIBYUC_CONTAINER_HASH_LIST_DEFINE(Cache, PageId, YUDB_CACHER_LRU_LIST_ACCESSOR, LIBYUC_OBJECT_ALLOCATOR_DEFALUT, YUDB_CAHCER_LRU_LIST_HASHER, LIBYUC_OBJECT_COMPARER_DEFALUT)


#define YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR_GetNext(list, cache_info) ((cache_info)->free_entry.next)
#define YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR_GetPrev(list, cache_info) ((cache_info)->free_entry.prev)
#define YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR_SetNext(list, cache_info, new_next) ((cache_info)->free_entry.next = (new_next))
#define YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR_SetPrev(list, cache_info, new_prev) ((cache_info)->free_entry.prev = (new_prev))
#define YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR
LIBYUC_CONTAINER_DOUBLY_STATIC_LIST_DEFINE(Cache, int16_t, CacheInfo, LIBYUC_CONTAINER_DOUBLY_STATIC_LIST_DEFAULT_REFERENCER, YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR, 2)


#define LIBYUC_CONTINUE_RB_TREE_ACCESSOR_DEFALUT_GetKey(TREE, ENTRY) (&ObjectGetFromField((ENTRY), CacheInfo, dirty_entry)->pgid)
#define YUDB_WRITE_QUEUE_RB_TREE_ACCESSOR LIBYUC_CONTINUE_RB_TREE_ACCESSOR_DEFALUT
LIBYUC_CONTAINER_RB_TREE_DEFINE(Cache, struct _CacheRbEntry*, PageId, LIBYUC_OBJECT_REFERENCER_DEFALUT, LIBYUC_CONTINUE_RB_TREE_ACCESSOR_DEFALUT, LIBYUC_OBJECT_COMPARER_DEFALUT)


inline CacheInfo* CacherGetInfo(Cacher* cacher, CacheId id) {
	return &cacher->cache_info_pool->obj_arr[id];
}

CacheId CacherGetIdByBuf(Cacher* cacher, void* cache) {
	Pager* pager = ObjectGetFromField(cacher, Pager, cacher);
	uintptr_t offset = (uintptr_t)cache - (uintptr_t)cacher->cache_pool;
	return (CacheId)(offset / pager->page_size);
}

CacheId CacherGetIdByInfo(Cacher* cacher, CacheInfo* info) {
	return info - cacher->cache_info_pool->obj_arr;
}

PageId CacherGetPageIdById(Cacher* cacher, CacheId id) {
	CacheInfo* info = CacherGetInfo(cacher, id);
	return info->pgid;
}



static void CacherWriteLaterThread(Cacher* cacher) {
	YuDb* db = ObjectGetFromField(cacher, YuDb, pager.cacher);
	do {
		ThreadSwitch();    // Sleep(db->config.wal_write_thread_disk_drop_interval);
		
		RwLockWriteAcquire(&cacher->write_later_queue_lock);
		// 优先处理不可变队列，不可变队列清空时表示检查点落盘任务完成
		CacheRbTree* tree;
		if (cacher->immutable_write_later_tree.root != NULL) {
			tree = &cacher->immutable_write_later_tree;
		} else if(cacher->write_later_tree.root != NULL) {
			tree = &cacher->write_later_tree;
		} else {
			RwLockWriteRelease(&cacher->write_later_queue_lock);
			continue;
		}
		CacheRbEntry* entry = CacheRbTreeIteratorFirst(tree);
		while (entry) {
			CacheInfo* cache_info = ObjectGetFromField(entry, CacheInfo, immutable_write_later_entry);
			CacheRbEntry* next_entry = CacheRbTreeIteratorNext(tree, entry);
			CacheRbTreeDelete(tree, entry);
			entry = next_entry;
			RwLockWriteRelease(&cacher->write_later_queue_lock);		// 落盘前释放锁

			CacheId cache_id = CacherGetIdByInfo(cacher, cache_info);
			void* cache = CacherGet(cacher, cache_id);
			PagerWrite(&db->pager, cache_info->pgid, cache, 1);

			RwLockWriteAcquire(&cacher->hotspot_queue_lock);
			CacheDoublyStaticListPush(cacher->cache_info_pool, kCacheTypeFree, cache_id);
			RwLockWriteRelease(&cacher->hotspot_queue_lock);
			RwLockWriteAcquire(&cacher->write_later_queue_lock);
		}
		
		if (tree == &cacher->immutable_write_later_tree) {
			// 检查点落盘任务完成，进行同步收尾

		}
		RwLockWriteRelease(&cacher->write_later_queue_lock);
	} while (cacher->write_thread_status != kCacheWriteThreadStop);
	cacher->write_thread_status = kCacheWriteThreadDestroy;
}

static void CacherEvict(Cacher* cacher, CacheId cache_id) {
	YuDb* db = ObjectGetFromField(cacher, YuDb, pager.cacher);
	Pager* pager = &db->pager;
	
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
 	  assert(cache_info->reference_count == 0);		// 被驱逐的缓存引用计数必须为0
	if (cache_info->type == kCacheTypeDirty) {
		// 是脏页则写回磁盘
		if (db->config.update_mode == kConfigUpdateInPlace) {
			void* cache = CacherGet(cacher, cache_id);
			PagerWrite(pager, cache_info->pgid, cache, 1);
		}
		// 会从缓存列表中移除，不需要再解引用了
	}
	RwLockWriteRelease(&cacher->hotspot_queue_lock);
	CacherFree(cacher, cache_id);
	RwLockWriteAcquire(&cacher->hotspot_queue_lock);
}


static inline size_t CacherFastMapHash(PageId pgid) {
	return  pgid % kCacherFastMapCount;
}


void CacherInit(Cacher* cacher, size_t count) {
	Pager* pager = ObjectGetFromField(cacher, Pager, cacher);

	cacher->cache_pool = malloc(pager->page_size * count);
	cacher->cache_info_pool = malloc(sizeof(CacheDoublyStaticList) + sizeof(CacheInfo) * count);
	CacheDoublyStaticListInit(cacher->cache_info_pool, count);
	CacheHashListInit(&cacher->lru_list, count, &kPageInvalidId);
	CacheRbTreeInit(&cacher->dirty_tree);
	for (int i = 0; i < sizeof(cacher->fast_map) / sizeof(*cacher->fast_map); i++) {
		cacher->fast_map[i].pgid = kPageInvalidId;
	}

	YuDb* db = ObjectGetFromField(pager, YuDb, pager);
	if (db->config.update_mode == kConfigUpdateWal) {
		CacheRbTreeInit(&cacher->write_later_tree);
		CacheRbTreeInit(&cacher->immutable_write_later_tree);
		RwLockInit(&cacher->hotspot_queue_lock);
		RwLockInit(&cacher->write_later_queue_lock);
		cacher->write_later_tx_count = 0;
		cacher->write_thread_status = kCacheWriteThreadReady;
		ThreadCreate(CacherWriteLaterThread, cacher);
	}
}

/*
* 从缓存管理器中分配一页缓存
*/
CacheId CacherAlloc(Cacher* cacher, PageId pgid) {
	YuDb* db = ObjectGetFromField(cacher, YuDb, pager.cacher);
	RwLockWriteAcquire(&cacher->hotspot_queue_lock);
	CacheId cache_id = CacheDoublyStaticListPop(cacher->cache_info_pool, kCacheTypeFree);
	CacheInfo* evict_cache_info;
	size_t cur_count = cacher->lru_list.hash_table.bucket.count;
	size_t max_count = cacher->lru_list.max_count;
	// 这里不知道什么原因，release下在cur_count == max_count时，CacheDoublyStaticListPop返回的是kCacheInvalidId，故+1
	if (cur_count + 1 >= max_count * db->config.hotspot_queue_full_percentage / 100) {
		// 需要从热点队列驱逐缓存
		CacheHashListEntry* lru_entry = CacheHashListPop(&cacher->lru_list);
		  assert(lru_entry != NULL);
		evict_cache_info = ObjectGetFromField(lru_entry, CacheInfo, lru_entry);
		CacherEvict(cacher, CacherGetIdByInfo(cacher, evict_cache_info));
		if (cache_id == kCacheInvalidId) {
		retry:
			cache_id = CacheDoublyStaticListPop(cacher->cache_info_pool, kCacheTypeFree);
			if (db->config.update_mode == kConfigUpdateWal) {
				// Wal模式下，缓存页面可能未来得及落盘导致没有空闲缓存页，需要多次尝试
				if (cache_id == kCacheInvalidId) {
					MutexLockRelease(&cacher->hotspot_queue_lock);
					ThreadSwitch();
					MutexLockAcquire(&cacher->hotspot_queue_lock);
					goto retry;
				}
			}
		}
	}
	  assert(cache_id != kCacheInvalidId);
	// 新分配的缓存挂到干净的链表上
	CacheDoublyStaticListPush(cacher->cache_info_pool, kCacheTypeClean, cache_id);
	// 同时挂到Lru中
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	cache_info->pgid = pgid;
	cache_info->reference_count = 0;
	cache_info->type = kCacheTypeClean;
	CacheHashListEntry* lru_entry = CacheHashListPut(&cacher->lru_list, &cache_info->lru_entry);
	  assert(lru_entry == NULL);		// 不应该会推入已在链表的缓存
	RwLockWriteRelease(&cacher->hotspot_queue_lock);
	
	size_t fast_map_index = CacherFastMapHash(pgid);
	if (cacher->fast_map[fast_map_index].pgid == kPageInvalidId) {
		cacher->fast_map[fast_map_index].pgid = pgid;
		cacher->fast_map[fast_map_index].cacheid = cache_id;
	}
	
	return cache_id;
}

/*
* 向缓存池释放分配一页分配的缓存
*/
void CacherFree(Cacher* cacher, CacheId cache_id) {
	YuDb* db = ObjectGetFromField(cacher, YuDb, pager.cacher);
	
	// 原子比较等待引用计数归0
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);

	size_t fast_map_index = CacherFastMapHash(cache_info->pgid);
	if (cacher->fast_map[fast_map_index].pgid == cache_info->pgid) {
		cacher->fast_map[fast_map_index].pgid = kPageInvalidId;
	}

	RwLockWriteAcquire(&cacher->hotspot_queue_lock);
	CacheHashListEntry* del_entry = CacheHashListDelete(&cacher->lru_list, &cache_info->pgid);
	//  assert(del_entry);
	if (cache_info->type == kCacheTypeClean) {
		CacheDoublyStaticListDelete(cacher->cache_info_pool, cache_info->type, cache_id);
	} else if (cache_info->type == kCacheTypeDirty) {
		CacheRbTreeDelete(&cacher->dirty_tree, &cache_info->dirty_entry);
	} else {
		  assert(0);
	}
	if (db->config.update_mode == kConfigUpdateWal) {
		// 挂到稍后写队列
		RwLockWriteAcquire(&cacher->write_later_queue_lock);
		CacheRbTreePut(&cacher->write_later_tree, &cache_info->write_later_entry);
		RwLockWriteRelease(&cacher->write_later_queue_lock);
	} else {
		CacheDoublyStaticListPush(cacher->cache_info_pool, kCacheTypeFree, cache_id);
	}
	RwLockWriteRelease(&cacher->hotspot_queue_lock);
	cache_info->type = kCacheTypeFree;
}

/*
* 查找缓存映射的页面
*/
CacheId CacherFind(Cacher* cacher, PageId pgid, bool put_first) {
	YuDb* db = ObjectGetFromField(cacher, YuDb, pager.cacher);

	size_t fast_map_index = CacherFastMapHash(pgid);
	if (cacher->fast_map[fast_map_index].pgid == pgid) {
		return cacher->fast_map[fast_map_index].cacheid;
	}

	RwLockReadAcquire(&cacher->hotspot_queue_lock);
	CacheHashListEntry* lru_entry = CacheHashListGet(&cacher->lru_list, &pgid, put_first);
	CacheInfo* cache_info;
	if (!lru_entry) {
		if (db->config.update_mode != kConfigUpdateWal) {
			RwLockReadRelease(&cacher->hotspot_queue_lock);
			return kCacheInvalidId;
		}
		RwLockReadAcquire(&cacher->write_later_queue_lock);
		CacheRbEntry* rb_entry = CacheRbTreeFind(&cacher->write_later_tree, &pgid);
		if (rb_entry == NULL) {
			rb_entry = CacheRbTreeFind(&cacher->immutable_write_later_tree, &pgid);
			if (rb_entry == NULL) {
				RwLockReadRelease(&cacher->write_later_queue_lock);
				RwLockReadRelease(&cacher->hotspot_queue_lock);
				return kCacheInvalidId;
			}
			// 位于不可变稍后写入队列，复制一份缓存
			cache_info = ObjectGetFromField(rb_entry, CacheInfo, write_later_entry);
			CacheId cache_id = CacherGetIdByInfo(cacher, cache_info);

			RwLockReadRelease(&cacher->hotspot_queue_lock);
			CacheId new_cache_id = CacherAlloc(cacher, pgid);
			RwLockReadAcquire(&cacher->hotspot_queue_lock);

			void* cache = CacherGet(cacher, cache_id);
			void* new_cache = CacherGet(cacher, new_cache_id);
			
			memcpy(new_cache, cache, db->pager.page_size);
		} else {
			// 移回热点队列
			CacheRbTreeDelete(&cacher->write_later_tree, rb_entry);
			CacheRbTreePut(&cacher->dirty_tree, rb_entry);
			cache_info = ObjectGetFromField(rb_entry, CacheInfo, write_later_entry);
		}
		RwLockReadRelease(&cacher->write_later_queue_lock);
	}
	else {
		cache_info = ObjectGetFromField(lru_entry, CacheInfo, lru_entry);
	}
	RwLockReadRelease(&cacher->hotspot_queue_lock);

	CacheId cache_id = CacherGetIdByInfo(cacher, cache_info);

	cacher->fast_map[fast_map_index].pgid = pgid;
	cacher->fast_map[fast_map_index].cacheid = cache_id;

	return cache_id;
}

/*
* 引用一页缓存，返回其内存地址
*/
void* CacherGet(Cacher* cacher, CacheId cache_id) {
	Pager* pager = ObjectGetFromField(cacher, Pager, cacher);
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	++cache_info->reference_count;
	//AtomicIncrement32(&cache_info->reference_count);
	return (void*)(((uintptr_t)cacher->cache_pool) + cache_id * pager->page_size);
}

/*
* 取消引用缓存
*/
void CacherDereference(Cacher* cacher, CacheId cache_id) {
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	--cache_info->reference_count;
	// AtomicDecrement32(&cache_info->reference_count);
	  assert(cache_info->reference_count >= 0);
}


void CacherMarkDirty(Cacher* cacher, CacheId cache_id) {
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	if (cache_info->type == kCacheTypeClean) {
		RwLockWriteAcquire(&cacher->hotspot_queue_lock);
		CacheDoublyStaticListDelete(cacher->cache_info_pool, cache_info->type, cache_id);
		CacheRbTreePut(&cacher->dirty_tree, &cache_info->dirty_entry);
		cache_info->type = kCacheTypeDirty;
		RwLockWriteRelease(&cacher->hotspot_queue_lock);
	}
	else {
		  assert(cache_info->type == kCacheTypeDirty);
	}
	
}



/*
* 将当前队列封存为不可变队列，切换到新队列
* 若有上一个未完成落盘任务的不可变队列则会阻塞
*/
void CacherWriteLaterQueueImmutable(Cacher* cacher) {
	RwLockWriteAcquire(&cacher->write_later_queue_lock);
	while (cacher->immutable_write_later_tree.root != NULL) {
		RwLockWriteRelease(&cacher->write_later_queue_lock);
		ThreadSwitch();
		RwLockWriteAcquire(&cacher->write_later_queue_lock);
	}
	cacher->immutable_write_later_tree = cacher->write_later_tree;
	cacher->write_later_tree.root = NULL;
	cacher->write_later_tx_count = 0;
	RwLockWriteRelease(&cacher->write_later_queue_lock);
}