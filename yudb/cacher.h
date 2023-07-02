#ifndef YUDB_CACHER_H_
#define YUDB_CACHER_H_

#include <stdbool.h>
#include <stdint.h>

#include <CUtils/container/hash_list.h>
#include <CUtils/container/rb_tree.h>
#include <CUtils/container/doubly_static_list.h>
#include <CUtils/concurrency/rw_lock.h>

#include <yudb/page.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

CUTILS_CONTAINER_HASH_LIST_DECLARATION(Cache, PageId)

typedef int32_t CacheId;

typedef enum {
	kCacheTypeFree = 0,
	kCacheTypeClean = 1,
	kCacheTypeDirty = 2,

	kCacheTypeWriteLater = 3,
	kCacheTypeImmutableWriteLater = 4,
} CacheType;

CUTILS_CONTAINER_RB_TREE_DECLARATION(Cache, struct _CacheRbEntry*, PageId)
CUTILS_CONTAINER_DOUBLY_STATIC_LIST_DECLARATION_1(Cache, int16_t)

typedef enum _CacheWriteThreadStatus {
	kCacheWriteThreadReady,
	kCacheWriteThreadRunning,
	kCacheWriteThreadSuspend,
	kCacheWriteThreadStop,
	kCacheWriteThreadDestroy,
} CacheWriteThreadStatus;

typedef struct _CacheInfo {
	PageId pgid;				// 对应的页面id
	int32_t reference_count;	// 缓存引用计数
	CacheType type;
	CacheHashListEntry lru_entry;		// 基于LRU策略管理缓存页面
	union {
		union {
			CacheDoublyStaticListEntry free_entry;
			CacheDoublyStaticListEntry clean_entry;

			CacheRbEntry dirty_entry;

			/* 稍后落盘队列，wal模式使用 */
			CacheRbEntry write_later_entry;
			CacheRbEntry immutable_write_later_entry;
		};
	};
} CacheInfo;
CUTILS_CONTAINER_DOUBLY_STATIC_LIST_DECLARATION_2(Cache, int16_t, CacheInfo, 2)

#define kCacherFastMapCount 32

typedef struct _Cacher {
	RwLock hotspot_queue_lock;		// 热点队列锁
	CacheHashList lru_list;
	CacheRbTree dirty_tree;

	/* wal模式使用 */
	RwLock write_later_queue_lock;	// 稍后写入队列锁
	CacheRbTree write_later_tree;
	CacheRbTree immutable_write_later_tree;
	int32_t write_later_tx_count;		// 当前已进行的写事务计数，达到一定数量时就会触发队列封存
	CacheWriteThreadStatus write_thread_status;

	struct {
		PageId pgid;
		CacheId cacheid;
	} fast_map[kCacherFastMapCount];
	void* cache_pool;
	CacheDoublyStaticList* cache_info_pool;
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

void CacherWriteLaterQueueImmutable(Cacher* cacher);

extern const CacheId kCacheInvalidId;

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_CACHER_H_