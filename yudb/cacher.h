#ifndef YUDB_CACHER_H_
#define YUDB_CACHER_H_

#include <stdbool.h>
#include <stdint.h>

#include <CUtils/container/lru_list.h>
#include <CUtils/container/doubly_static_list.h>

#include <yudb/page.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

CUTILS_CONTAINER_LRU_LIST_DECLARATION(Cache, PageId)

typedef int32_t CacheId;

typedef enum {
	kCacheListFree = 0,
	kCacheListClean = 1,
	kCacheListDirty = 2,
} CacheListType;

CUTILS_CONTAINER_DOUBLY_STATIC_LIST_DECLARATION_1(Cache, int16_t)
typedef struct _CacheInfo {
	CacheListType type;
	union {
		CacheDoublyStaticListEntry free_entry;
		CacheDoublyStaticListEntry clean_entry;
		CacheDoublyStaticListEntry dirty_entry;
	};
	CacheLruListEntry lru_entry;
	PageId pgid;		// 对应的页面id
	uint32_t reference_count;
} CacheInfo;
CUTILS_CONTAINER_DOUBLY_STATIC_LIST_DECLARATION_2(Cache, int16_t, CacheInfo, 3)



typedef struct _Cacher {
	CacheLruList cache_lru_list;		// 基于LRU策略管理已被使用的缓存
	void* cache_pool;
	CacheDoublyStaticList cache_info_pool;
} Cacher;

void CacherInit(Cacher* cacher, size_t count);
CacheId CacherAlloc(Cacher* cacher, PageId pgid);
void CacherFree(Cacher* cacher, CacheId id);
CacheId CacherFind(Cacher* cacher, PageId pgid, bool put_first);
void* CacherGet(Cacher* cacher, CacheId id);
CacheInfo* CacherGetInfo(Cacher* cacher, CacheId id);
CacheId CacherGetIdByBuf(Cacher* cacher, void* cache);
CacheId CacherGetIdByInfo(Cacher* cacher, CacheInfo* info);
PageId CacherGetPageIdById(Cacher* cacher, CacheId id);
void CacherMarkDirty(Cacher* cacher, CacheId cache_id);
void* CacherGet(Cacher* cacher, CacheId cache_id);
void CacherDereference(Cacher* cacher, CacheId cache_id);

extern const CacheId kCacheInvalidId;

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_CACHER_H_