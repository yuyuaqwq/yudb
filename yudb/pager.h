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
} Pager;

extern const PageId kPageInvalidId;

bool PagerInit(Pager* pager, int16_t page_size, PageCount page_count, size_t cache_count);
void PagerWriteAllDirty(Pager* pager);
PageId PagerAlloc(Pager* pager, bool put_cache, PageCount count);
void PagerFree(Pager* pager, PageId pgid, PageCount count);
void PagerPending(Pager* pager, PageId pgid, PageCount count, PageId first_pgid);
void PagerFreePending(Pager* pager, PageId first_pgid);
bool PagerRead(Pager* pager, PageId pgid, void* cache, PageCount count);
bool PagerWrite(Pager* pager, PageId pgid, void* cache, PageCount count);
void* PagerGet(Pager* pager, PageId pgid);
void PagerDereference(Pager* pager, PageId pgid);
void PagerMarkDirty(Pager* pager, PageId pgid);


#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_PAGER_H_