#ifndef YUDB_PAGER_H_
#define YUDB_PAGER_H_

#include <stdio.h>
#include <stdint.h>

#include <yudb/page.h>
#include <yudb/txid.h>
#include <yudb/free_table.h>
#include <yudb/cacher.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

typedef struct _Pager {
	int16_t page_size;
	PageCount page_count;
	FreeTable free_table;		// 磁盘空闲页面管理表
	Cacher cacher;		// 缓存管理器
	PageIdVector free_page_pool;
} Pager;

bool PagerInit(Pager* pager, int16_t page_size, PageCount page_count, size_t cache_count);
PageId PagerAlloc(Pager* pager, bool put_cache, PageCount count);
void PagerFree(Pager* pager, PageId pgid, bool skip_pool);
void PagerPending(Pager* pager, struct _Tx* tx, PageId pgid);
bool PagerRead(Pager* pager, PageId pgid, void* cache, PageCount count);
bool PagerWrite(Pager* pager, PageId pgid, void* cache, PageCount count);
void* PagerReference(Pager* pager, PageId pgid);
void PagerDereference(Pager* pager, void* cache);
void PagerMarkDirty(Pager* pager, void* cache);
void PagerCleanFreePool(Pager* pager);
void PagerSyncWriteAllDirty(Pager* pager);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_PAGER_H_