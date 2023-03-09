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

typedef int32_t CacheId;

typedef enum {
	kCacheListFree = 0,
	kCacheListClean = 1,
	kCacheListDirty = 2,
} CacheListType;

typedef struct _CacheInfo {
	CacheListType type;
	union {
		DoublyStaticListEntry free_entry;
		DoublyStaticListEntry clean_entry;
		DoublyStaticListEntry dirty_entry;
	};
	LruEntry lru_entry;
	PageId pgid;
	int reference_count;
} CacheInfo;

typedef struct _Cacher {
	DoublyStaticList cache_info_pool;
	// CacheId cache_free_first;		// 连接所有未被使用的缓存页，在cache_info_pool中已经包含
	CacheId cache_clean_first;		// 连接所有已被分配的干净页
	CacheId cache_dirty_first;		// 连接所有已被分配的脏页
	void* cache_pool;
	LruList cache_lru_list;		// 基于LRU策略管理已被使用的缓存
} Cacher;

void CacherInit(Cacher* cacher, size_t count);
CacheId CacherAlloc(Cacher* cacher, PageId pgid);
void CacherFree(Cacher* cacher, CacheId id);
CacheId CacherFind(Cacher* cacher, PageId pgid, bool put_first);
void* CacherGet(Cacher* cacher, CacheId id);
CacheInfo* CacherGetInfo(Cacher* cacher, CacheId id);
CacheId CacherGetIdByBuf(Cacher* cacher, void* cache);
PageId CacherGetPageIdById(Cacher* cacher, CacheId id);
void CacherMarkDirty(Cacher* cacher, CacheId cache_id);
void* CacherGet(Cacher* cacher, CacheId cache_id);
void CacherDereference(Cacher* cacher, CacheId cache_id);

extern const CacheId kCacheInvalidId;

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_CACHER_H_