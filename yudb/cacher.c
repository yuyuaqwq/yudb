#include "yudb/cacher.h"

#include "yudb/pager.h"
#include "yudb/yudb.h"

#include <CUtils/algorithm/hash_map.h>

const CacheId kCacheInvalidId = -1;

#define YUDB_CACHE_LRU_LIST_ACCESSOR_GetKey(LIST, ENTRY) (ObjectGetFromField(&ENTRY, CacheInfo, lru_entry)->pgid)
#define YUDB_CACHE_LRU_LIST_ACCESSOR YUDB_CACHE_LRU_LIST_ACCESSOR
//CUTILS_CONTAINER_LRU_LIST_DEFINE(Cache, PageId, YUDB_CACHE_LRU_LIST_ACCESSOR, CUTILS_OBJECT_ALLOCATOR_DEFALUT, Hashmap_hashint, CUTILS_OBJECT_COMPARER_DEFALUT)
__forceinline PageId CacheLruHashEntryAccessor_GetKey(CacheLruListHashTable* table, CacheLruHashEntry hash_entry) {
	return (((CacheInfo*)((uintptr_t)(&*(hash_entry.lru_entry)) - ((int)&(((CacheInfo*)0)->lru_entry))))->pgid);
}
void CacheLruListHashTableVectorResetCapacity(CacheLruListHashTableVector* arr, size_t capacity) {
	CacheLruListHashEntry* new_buf = ((CacheLruListHashEntry*)MemoryAlloc(sizeof(CacheLruListHashEntry) * (capacity)));
	if (arr->obj_arr) {
		memcpy((void*)(new_buf), (void*)(arr->obj_arr), (sizeof(CacheLruListHashEntry) * arr->count));
		(MemoryFree(arr->obj_arr));
	}
	arr->obj_arr = new_buf;
	arr->capacity = capacity;
}
void CacheLruListHashTableVectorExpand(CacheLruListHashTableVector* arr, size_t add_count) {
	size_t cur_capacity = arr->capacity;
	size_t target_count = cur_capacity + add_count;
	if (cur_capacity == 0) {
		cur_capacity = 1;
	}
	while (cur_capacity < target_count) {
		cur_capacity *= 2;
	}
	CacheLruListHashTableVectorResetCapacity(arr, cur_capacity);
}
void CacheLruListHashTableVectorInit(CacheLruListHashTableVector* arr, size_t count, _Bool create) {
	arr->count = count;
	arr->obj_arr = ((void*)0);
	if (count != 0 && create) {
		CacheLruListHashTableVectorResetCapacity(arr, count);
	}
	else {
		arr->capacity = count;
	}
}
void CacheLruListHashTableVectorRelease(CacheLruListHashTableVector* arr) {
	if (arr->obj_arr) {
		(MemoryFree(arr->obj_arr));
		arr->obj_arr = ((void*)0);
	}
	arr->capacity = 0;
	arr->count = 0;
}
ptrdiff_t CacheLruListHashTableVectorPushTail(CacheLruListHashTableVector* arr, const CacheLruListHashEntry* obj) {
	if (arr->capacity <= arr->count) {
		CacheLruListHashTableVectorExpand(arr, 1);
	}
	memcpy((void*)(&arr->obj_arr[arr->count++]), (void*)(obj), (sizeof(CacheLruListHashEntry)));
	return arr->count - 1;
}
CacheLruListHashEntry* CacheLruListHashTableVectorPopTail(CacheLruListHashTableVector* arr) {
	if (arr->count == 0) {
		return ((void*)0);
	}
	return &arr->obj_arr[--arr->count];
}
static inline uint32_t CacheLruListHashGetIndex(CacheLruListHashTable* table, const PageId* key) {
	return Hashmap_hashint(table, *key) % table->bucket.capacity;
}
static inline uint32_t CacheLruListHashGetCurrentLoadFator(CacheLruListHashTable* table) {
	return table->bucket.count * 100 / table->bucket.capacity;
}
static void CacheLruListHashRehash(CacheLruListHashTable* table, size_t new_capacity) {
	CacheLruListHashTable temp_table;
	CacheLruListHashTableInit(&temp_table, new_capacity, table->load_fator);
	CacheLruListHashTableIterator iter;
	CacheLruHashEntry* obj = CacheLruListHashTableIteratorFirst(table, &iter);
	while (obj) {
		CacheLruListHashTablePut(&temp_table, obj);
		PageId key = CacheLruHashEntryAccessor_GetKey(table, *obj);
		CacheLruListHashTableDelete(table, &key);
		obj = CacheLruListHashTableIteratorNext(table, &iter);
	}
	CacheLruListHashTableVectorRelease(&table->bucket);
	memcpy((void*)(table), (void*)(&temp_table), (sizeof(temp_table)));
}
void CacheLruListHashTableInit(CacheLruListHashTable* table, size_t capacity, uint32_t load_fator) {
	if (capacity == 0) {
		capacity = 16;
	}
	CacheLruListHashTableVectorInit(&table->bucket, capacity, 1);
	table->bucket.count = 0;
	for (int i = 0; i < table->bucket.capacity; i++) {
		table->bucket.obj_arr[i].type = kHashEntryFree;
	}
	if (load_fator == 0) {
		load_fator = 75;
	}
	table->load_fator = load_fator;
}
void CacheLruListHashTableRelease(CacheLruListHashTable* table) {
	CacheLruListHashTableIterator iter;
	CacheLruHashEntry* obj = CacheLruListHashTableIteratorFirst(table, &iter);
	while (obj) {
		PageId key = CacheLruHashEntryAccessor_GetKey(table, *obj);
		CacheLruListHashTableDelete(table, &key);
		obj = CacheLruListHashTableIteratorNext(table, &iter);
	}
	CacheLruListHashTableVectorRelease(&table->bucket);
}
size_t CacheLruListHashTableGetCount(CacheLruListHashTable* table) {
	return table->bucket.count;
}
CacheLruHashEntry* CacheLruListHashTableFind(CacheLruListHashTable* table, const PageId* key) {
	uint32_t index = CacheLruListHashGetIndex(table, key);
	CacheLruListHashEntry* entry = &table->bucket.obj_arr[index];
	if (entry->type == kHashEntryObj) {
		if (((CacheLruHashEntryAccessor_GetKey(table, entry->obj)) == (*key))) {
			return &entry->obj;
		}
	}
	else if (entry->type == kHashEntryList) {
		SinglyListEntry* cur = SinglyListIteratorFirst(&entry->list_head);
		while (cur) {
			CacheLruListHashEntryList* list_entry = (CacheLruListHashEntryList*)cur;
			if (((CacheLruHashEntryAccessor_GetKey(table, list_entry->obj)) == (*key))) {
				return &entry->obj;
			}
			cur = SinglyListIteratorNext(&entry->list_head, cur);
		}
	}
	return ((void*)0);
}
_Bool CacheLruListHashTablePut(CacheLruListHashTable* table, const CacheLruHashEntry* obj) {
	PageId key = CacheLruHashEntryAccessor_GetKey(table, *obj);
	uint32_t index = CacheLruListHashGetIndex(table, &key);
	CacheLruListHashEntry* entry = &table->bucket.obj_arr[index];
	if (entry->type == kHashEntryFree) {
		((entry->obj) = (*obj));
		entry->type = kHashEntryObj;
	}
	else if (entry->type == kHashEntryObj) {
		if (((CacheLruHashEntryAccessor_GetKey(table, entry->obj)) == (CacheLruHashEntryAccessor_GetKey(table, *obj)))) {
			((entry->obj) = (*obj));
			return 1;
		}
		entry->type = kHashEntryList;
		CacheLruListHashEntryList* list_entry = ((CacheLruListHashEntryList*)MemoryAlloc(sizeof(CacheLruListHashEntryList)));
		((list_entry->obj) = (entry->obj));
		SinglyListHeadInit(&entry->list_head);
		SinglyListPutFirst(&entry->list_head, &list_entry->list_entry);
		list_entry = ((CacheLruListHashEntryList*)MemoryAlloc(sizeof(CacheLruListHashEntryList)));
		((list_entry->obj) = (*obj));
		SinglyListPutFirst(&entry->list_head, &list_entry->list_entry);
	}
	else if (entry->type == kHashEntryList) {
		SinglyListEntry* cur = SinglyListIteratorFirst(&entry->list_head);
		while (cur) {
			CacheLruListHashEntryList* list_entry = (CacheLruListHashEntryList*)cur;
			if (((CacheLruHashEntryAccessor_GetKey(table, list_entry->obj)) == (CacheLruHashEntryAccessor_GetKey(table, *obj)))) {
				((list_entry->obj) = (*obj));
				break;
			}
			cur = SinglyListIteratorNext(&entry->list_head, cur);
		}
		if (!cur) {
			CacheLruListHashEntryList* list_entry = ((CacheLruListHashEntryList*)MemoryAlloc(sizeof(CacheLruListHashEntryList)));
			((list_entry->obj) = (*obj));
			SinglyListPutFirst(&entry->list_head, &list_entry->list_entry);
		}
	}
	table->bucket.count++;
	if (CacheLruListHashGetCurrentLoadFator(table) >= table->load_fator) {
		CacheLruListHashRehash(table, table->bucket.capacity * 2);
	}
	return 1;
}
_Bool CacheLruListHashTableDelete(CacheLruListHashTable* table, const PageId* key) {
	uint32_t index = CacheLruListHashGetIndex(table, key);
	CacheLruListHashEntry* entry = &table->bucket.obj_arr[index];
	_Bool success = 1;
	if (entry->type == kHashEntryFree) {
		return 0;
	}
	else if (entry->type == kHashEntryObj) {
		if (!((CacheLruHashEntryAccessor_GetKey(table, entry->obj)) == (*key))) {
			return 0;
		}
		entry->type = kHashEntryFree;
		success = 1;
	}
	else if (entry->type == kHashEntryList) {
		SinglyListEntry* prev = ((void*)0);
		SinglyListEntry* cur = SinglyListIteratorFirst(&entry->list_head);
		while (cur) {
			if (!((CacheLruHashEntryAccessor_GetKey(table, ((CacheLruListHashEntryList*)cur)->obj)) == (*key))) {
				prev = cur;
				cur = SinglyListIteratorNext(&entry->list_head, cur);
				continue;
			}
			if (prev) {
				SinglyListDeleteEntry(&entry->list_head, prev, cur);
			}
			else {
				SinglyListDeleteFirst(&entry->list_head);
				if (SinglyListIsEmpty(&entry->list_head)) {
					entry->type = kHashEntryFree;
				}
			}
			(MemoryFree(cur));
			success = 1;
			break;
		}
	}
	if (success) table->bucket.count--;
	return success;
}
CacheLruHashEntry* CacheLruListHashTableIteratorFirst(CacheLruListHashTable* table, CacheLruListHashTableIterator* iter) {
	iter->cur_list_entry = ((void*)0);
	iter->cur_index = 0;
	return CacheLruListHashTableIteratorNext(table, iter);
}
CacheLruHashEntry* CacheLruListHashTableIteratorNext(CacheLruListHashTable* table, CacheLruListHashTableIterator* iter) {
	if (iter->cur_list_entry) {
		CacheLruListHashEntryList* cur = iter->cur_list_entry;
		CacheLruListHashEntry* entry = &table->bucket.obj_arr[iter->cur_index];
		iter->cur_list_entry = (CacheLruListHashEntryList*)SinglyListIteratorNext(&entry->list_head, &cur->list_entry);
		if (iter->cur_list_entry == ((void*)0)) {
			iter->cur_index++;
		}
		if (cur) {
			return &cur->obj;
		}
	}
	for (; iter->cur_index < table->bucket.capacity; iter->cur_index++) {
		CacheLruListHashEntry* entry = &table->bucket.obj_arr[iter->cur_index];
		if (entry->type == kHashEntryFree) {
			continue;
		}
		if (entry->type == kHashEntryObj) {
			iter->cur_index++;
			return &entry->obj;
		}
		if (entry->type == kHashEntryList) {
			iter->cur_list_entry = (CacheLruListHashEntryList*)entry->list_head.first;
			return CacheLruListHashTableIteratorNext(table, iter);
		}
	}
	return ((void*)0);
}
void CacheLruListInit(CacheLruList* list, size_t max_count) {
	CacheLruListHashTableInit(&list->hash_table, max_count, 0);
	ListInit(&list->list_head);
	list->max_count = max_count;
}
CacheLruListEntry* CacheLruListGet(CacheLruList* list, PageId* key, _Bool put_first) {
	CacheLruHashEntry* hash_entry = CacheLruListHashTableFind(&list->hash_table, key);
	if (!hash_entry) {
		return ((void*)0);
	}
	if (put_first) {
		ListPutFirst(&list->list_head, ListDeleteEntry(&list->list_head, &hash_entry->lru_entry->list_entry));
	}
	return hash_entry->lru_entry;
}
CacheLruListEntry* CacheLruListPut(CacheLruList* list, CacheLruListEntry* entry) {
	PageId key = (((CacheInfo*)((uintptr_t)(&*entry) - ((int)&(((CacheInfo*)0)->lru_entry))))->pgid);
	CacheLruHashEntry* hash_entry = CacheLruListHashTableFind(&list->hash_table, &key);
	if (hash_entry) {
		CacheLruListDelete(list, &key);
	}
	else if (CacheLruListHashTableGetCount(&list->hash_table) >= list->max_count) {
		hash_entry = CacheLruListPop(list);
	}
	CacheLruHashEntry put_hash_entry;
	put_hash_entry.lru_entry = entry;
	CacheLruListHashTablePut(&list->hash_table, &put_hash_entry);
	ListPutFirst(&list->list_head, &entry->list_entry);
	return hash_entry ? hash_entry->lru_entry : ((void*)0);
}
CacheLruListEntry* CacheLruListPop(CacheLruList* list) {
	ListEntry* del_list_entry = ListDeleteLast(&list->list_head);
	PageId key = (((CacheInfo*)((uintptr_t)(&*(CacheLruListEntry*)del_list_entry) - ((int)&(((CacheInfo*)0)->lru_entry))))->pgid);
	CacheLruListHashTableDelete(&list->hash_table, &key);
	return (CacheLruListEntry*)del_list_entry;
}
CacheLruListEntry* CacheLruListDelete(CacheLruList* list, PageId* key) {
	CacheLruListEntry* lru_entry = ((void*)0);
	CacheLruHashEntry* del_hash_entry = CacheLruListHashTableFind(&list->hash_table, key);
	if (del_hash_entry) {
		lru_entry = del_hash_entry->lru_entry;
		ListDeleteEntry(&list->list_head, &del_hash_entry->lru_entry->list_entry);
		CacheLruListHashTableDelete(&list->hash_table, key);
	}
	return del_hash_entry ? lru_entry : ((void*)0);
}


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

CacheId CacherAlloc(Cacher* cacher, PageId pgid) {
	CacheId cache_id = CacheDoublyStaticListPop(cacher->cache_info_pool, kCacheListFree);
	CacheInfo* evict_cache_info;
	if (cache_id == kCacheInvalidId) {
		CacheLruListEntry* lru_entry = CacheLruListPop(&cacher->cache_lru_list);
		  assert(lru_entry != NULL);
		evict_cache_info = ObjectGetFromField(lru_entry, CacheInfo, lru_entry);
		CacherEvict(cacher, CacherGetIdFromInfo(cacher, evict_cache_info));
		cache_id = CacheDoublyStaticListPop(cacher->cache_info_pool, kCacheListFree);
		  assert(cache_id != kCacheInvalidId);
	}
	CacheDoublyStaticListPush(cacher->cache_info_pool, kCacheListClean, cache_id);
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	cache_info->pgid = pgid;
	cache_info->reference_count = 0;
	cache_info->type = kCacheListClean;
	CacheLruListEntry* lru_entry = CacheLruListPut(&cacher->cache_lru_list, &cache_info->lru_entry);
	  assert(lru_entry == NULL);
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
	CacheLruListEntry* lru_entry = CacheLruListGet(&cacher->cache_lru_list, &pgid, put_first);
	if (!lru_entry) {
		return kCacheInvalidId;
	}
	CacheInfo* cache_info = ObjectGetFromField(lru_entry, CacheInfo, lru_entry);
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

