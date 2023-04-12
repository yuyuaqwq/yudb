#include "yudb/cacher.h"

#include "yudb/pager.h"
#include "yudb/yudb.h"

#include <CUtils/algorithm/hash_map.h>

const CacheId kCacheInvalidId = -1;

#define YUDB_CACHER_LRU_LIST_ACCESSOR_GetKey(LIST, ENTRY) (ObjectGetFromField(&ENTRY, CacheInfo, lru_entry)->pgid)
#define YUDB_CACHER_LRU_LIST_ACCESSOR YUDB_CACHER_LRU_LIST_ACCESSOR
#define YUDB_CAHCER_LRU_LIST_HASHER(TABLE, KEY) Hashmap_hashint(KEY)
CUTILS_CONTAINER_LRU_LIST_DEFINE(Cache, PageId, YUDB_CACHER_LRU_LIST_ACCESSOR, CUTILS_OBJECT_ALLOCATOR_DEFALUT, YUDB_CAHCER_LRU_LIST_HASHER, CUTILS_OBJECT_COMPARER_DEFALUT)


#define YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR_GetNext(list, cache_info) ((cache_info).entry.next)
#define YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR_GetPrev(list, cache_info) ((cache_info).entry.prev)
#define YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR_SetNext(list, cache_info, new_next) ((cache_info).entry.next = (new_next))
#define YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR_SetPrev(list, cache_info, new_prev) ((cache_info).entry.prev = (new_prev))
#define YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR
CUTILS_CONTAINER_DOUBLY_STATIC_LIST_DEFINE(Cache, int16_t, CacheInfo, CUTILS_OBJECT_REFERENCER_DEFALUT, YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR, 3)

static inline CacheId CacherGetIdFromInfo(Cacher* cacher, CacheInfo* info) {
	return info - (CacheInfo*)cacher->cache_info_pool->obj_arr;
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
	cacher->cache_info_pool = malloc(sizeof(CacheDoublyStaticList) + sizeof(CacheInfo) * count);
	CacheDoublyStaticListInit(cacher->cache_info_pool, count);
	CacheLruListInit(&cacher->cache_lru_list, count);
}

/*
* 从缓存管理器中分配一页缓存
*/
CacheId CacherAlloc(Cacher* cacher, PageId pgid) {
	CacheId cache_id = CacheDoublyStaticListPop(cacher->cache_info_pool, kCacheListFree);
	CacheInfo* evict_cache_info;
	if (cache_id == kCacheInvalidId) {
		// 缓存已满，驱逐lur末尾缓存
		CacheLruListEntry* lru_entry = CacheLruListPop(&cacher->cache_lru_list);
		  assert(lru_entry != NULL);
		evict_cache_info = ObjectGetFromField(lru_entry, CacheInfo, lru_entry);
		CacherEvict(cacher, CacherGetIdFromInfo(cacher, evict_cache_info));
		cache_id = CacheDoublyStaticListPop(cacher->cache_info_pool, kCacheListFree);
		  assert(cache_id != kCacheInvalidId);
	}
	// 新分配的缓存挂到干净的链表上
	CacheDoublyStaticListPush(cacher->cache_info_pool, kCacheListClean, cache_id);
	// 同时挂到Lru中
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	cache_info->pgid = pgid;
	cache_info->reference_count = 0;
	cache_info->type = kCacheListClean;
	CacheLruListEntry* lru_entry = CacheLruListPut(&cacher->cache_lru_list, &cache_info->lru_entry);
	  assert(lru_entry == NULL);		// Lru不应该还会驱逐
	evict_cache_info = ObjectGetFromField(lru_entry, CacheInfo, lru_entry);
	return cache_id;
}

void CacherFree(Cacher* cacher, CacheId cache_id) {
	// 原子比较等待引用计数归0
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	CacheLruListDelete(&cacher->cache_lru_list, &cache_info->pgid);
	CacheDoublyStaticListSwitch(cacher->cache_info_pool, cache_info->type, cache_id, kCacheListFree);
	cache_info->type = kCacheListFree;
}

CacheId CacherFind(Cacher* cacher, PageId pgid, bool put_first) {
	size_t fast_map_index = pgid % (sizeof(cacher->fast_map) / sizeof(*cacher->fast_map));
	if (cacher->fast_map[fast_map_index].pgid == pgid) {
		return cacher->fast_map[fast_map_index].cacheid;
	}
	CacheLruListEntry* lru_entry = CacheLruListGet(&cacher->cache_lru_list, &pgid, put_first);
	if (!lru_entry) {
		return kCacheInvalidId;
	}
	CacheInfo* cache_info = ObjectGetFromField(lru_entry, CacheInfo, lru_entry);
	cacher->fast_map[fast_map_index].pgid = pgid;
	cacher->fast_map[fast_map_index].cacheid = CacherGetIdFromInfo(cacher, cache_info);
	return cacher->fast_map[fast_map_index].cacheid;
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

inline CacheInfo* CacherGetInfo(Cacher* cacher, CacheId id) {
	return (CacheInfo*)&cacher->cache_info_pool->obj_arr[id];
}

CacheId CacherGetIdByBuf(Cacher* cacher, void* cache) {
	Pager* pager = ObjectGetFromField(cacher, Pager, cacher);
	uintptr_t offset = (uintptr_t)cache - (uintptr_t)cacher->cache_pool;
	return (CacheId)(offset / pager->page_size);
}

CacheId CacherGetIdByInfo(Cacher* cacher, CacheInfo* info) {
	Pager* pager = ObjectGetFromField(cacher, Pager, cacher);
	uintptr_t offset = (uintptr_t)info - (uintptr_t)cacher->cache_info_pool->obj_arr;
	return (CacheId)(offset / sizeof(CacheInfo));
}

PageId CacherGetPageIdById(Cacher* cacher, CacheId id) {
	CacheInfo* info = CacherGetInfo(cacher, id);
	return info->pgid;
}

void CacherMarkDirty(Cacher* cacher, CacheId cache_id) {
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	if (cache_info->type != kCacheListDirty) {
		CacheDoublyStaticListSwitch(cacher->cache_info_pool, cache_info->type, cache_id, kCacheListDirty);
		cache_info->type = kCacheListDirty;
	}
}

