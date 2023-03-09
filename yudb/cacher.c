#include "yudb/cacher.h"

#include "yudb/pager.h"
#include "yudb/yudb.h"

const CacheId kCacheInvalidId = -1;

static CacheId CacherGetIdFromInfo(Cacher* cacher, CacheInfo* info) {
	return info - (CacheInfo*)cacher->cache_info_pool.array.objArr;
}

static void CacherEvict(Cacher* cacher, CacheId cache_id) {
	Pager* pager = ObjectGetFromField(cacher, Pager, cacher);
	CacheInfo* cache_info = CacherGetInfo(&pager->cacher, cache_id);
	  assert(cache_info->reference_count == 0);		// 被驱逐的缓存引用计数必须为0
	if (cache_info->type == kCacheListDirty) {
		// 是脏页则写回磁盘
		void* cache = CacherGet(&pager->cacher, cache_id);
		PagerWrite(pager, cache_info->pgid, cache, 1);
		// 会从缓存列表中移除，不需要再解引用了
	}
	CacherFree(&pager->cacher, cache_id);
}

void CacherInit(Cacher* cacher, size_t count) {
	Pager* pager = ObjectGetFromField(cacher, Pager, cacher);
	cacher->cache_pool = malloc(pager->page_size * count);
	DoublyStaticListInit(&cacher->cache_info_pool, count, sizeof(CacheInfo), ObjectGetFieldOffset(CacheInfo, free_entry), 3);
	LruListInitByField(&cacher->cache_lru_list, count, CacheInfo, lru_entry, pgid);
}

CacheId CacherAlloc(Cacher* cacher, PageId pgid) {
	Pager* pager = ObjectGetFromField(cacher, Pager, cacher);

	CacheId cache_id = DoublyStaticListPop(&cacher->cache_info_pool, kCacheListFree);
	CacheInfo* evict_cache_info;
	if (cache_id == kCacheInvalidId) {
		evict_cache_info = (CacheInfo*)LruListPop(&cacher->cache_lru_list);
		  assert(evict_cache_info != NULL);
		CacherEvict(cacher, CacherGetIdFromInfo(cacher, evict_cache_info));
		cache_id = DoublyStaticListPop(&cacher->cache_info_pool, kCacheListFree);
		  assert(cache_id != kCacheInvalidId);
	}
	DoublyStaticListPush(&cacher->cache_info_pool, kCacheListClean, cache_id);
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	cache_info->pgid = pgid;
	cache_info->reference_count = 0;
	cache_info->type = kCacheListClean;
	evict_cache_info = (CacheInfo*)LruListPut(&cacher->cache_lru_list, &cache_info->lru_entry);
	  assert(evict_cache_info == NULL);
	return cache_id;
}

void CacherFree(Cacher* cacher, CacheId cache_id) {
	// 原子比较等待引用计数归0
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	LruListDelete(&cacher->cache_lru_list, &cache_info->pgid);
	DoublyStaticListSwitch(&cacher->cache_info_pool, cache_info->type, cache_id, kCacheListFree);
	cache_info->type = kCacheListFree;
}

CacheId CacherFind(Cacher* cacher, PageId pgid, bool put_first) {
	CacheInfo* cache_info = (CacheInfo*)LruListGet(&cacher->cache_lru_list, &pgid, put_first);
	if (!cache_info) {
		return kCacheInvalidId;
	}
	return CacherGetIdFromInfo(cacher, cache_info);
}

void* CacherGet(Cacher* cacher, CacheId cache_id) {
	Pager* pager = ObjectGetFromField(cacher, Pager, cacher);
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	cache_info->reference_count++;
	return (void*)(((uintptr_t)cacher->cache_pool) + cache_id * pager->page_size);
}

void CacherDereference(Cacher* cacher, CacheId cache_id) {
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	cache_info->reference_count--;
}

CacheInfo* CacherGetInfo(Cacher* cacher, CacheId id) {
	return DoublyStaticListAt(&cacher->cache_info_pool, id, CacheInfo);
}

CacheId CacherGetIdByBuf(Cacher* cacher, void* cache) {
	Pager* pager = ObjectGetFromField(cacher, Pager, cacher);
	uintptr_t offset = (uintptr_t)cache - (uintptr_t)cacher->cache_pool;
	return (CacheId)(offset / pager->page_size);
}

PageId CacherGetPageIdById(Cacher* cacher, CacheId id) {
	CacheInfo* info = CacherGetInfo(cacher, id);
	return info->pgid;
}

void CacherMarkDirty(Cacher* cacher, CacheId cache_id) {
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	if (cache_info->type != kCacheListDirty) {
		DoublyStaticListSwitch(&cacher->cache_info_pool, cache_info->type, cache_id, kCacheListDirty);
		cache_info->type = kCacheListDirty;
	}
}

