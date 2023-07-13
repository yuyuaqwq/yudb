#include <yudb/pager.h>

#include <libyuc/concurrency/thread.h>

#include <yudb/db_file.h>
#include <yudb/free_table.h>
#include <yudb/yudb.h>


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
    PageIdVectorInit(&pager->free_pgid_pool, 4, true);
    pager->free_pgid_pool.count = 0;

    pager->temp_page = MemoryAlloc(page_size);

    return true;
}

void PagerDestroy(Pager* pager) {
    MemoryFree(pager->temp_page);
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
* 页面写回到db文件
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
/*
* 分配大于4页的页面时，采取分段分配，即根据多个2的幂，可以组合成任意数的数学原理
*/
PageId PagerAlloc(Pager* pager, bool put_cache, PageCount count){
    PageId disk_free_pgid;
    YuDb* db = ObjectGetFromField(pager, YuDb, pager);
    if (db->config.update_mode == kConfigUpdateWal && pager->free_pgid_pool.count > 0) {
        disk_free_pgid = *PageIdVectorPopTail(&pager->free_pgid_pool);
          assert(disk_free_pgid != kPageInvalidId);
    }
    else {
        YuDb* db = ObjectGetFromField(pager, YuDb, pager);
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
                    FreeTableFree(&pager->free_table, disk_free_pgid);
                    return kPageInvalidId;
                }
            }
        }
    }
    return disk_free_pgid;
}

/*
* 指定页面置为待决
*/
void PagerPending(Pager* pager, Tx* tx, PageId pgid) {
    YuDb* db = ObjectGetFromField(pager, YuDb, pager);
    if (db->config.update_mode == kConfigUpdateWal) {
        if (db->tx_manager.last_persistent_txid == tx->meta_info.txid) {
            // 放到保留页号池
            PageIdVectorPushTail(&pager->reserve_pgid_pool, &pgid);
            return;
        }
    }
    TxRbEntry* entry = TxRbTreeFind(&tx->db->tx_manager.write_tx_record, &tx->meta_info.txid);
      assert(entry != NULL);
    TxWriteRecordEntry* pending_list_entry = ObjectGetFromField(entry, TxWriteRecordEntry, rb_entry);
    PageIdVectorPushTail(&pending_list_entry->pending_pgid_arr, &pgid);
    if (db->config.update_mode != kConfigUpdateWal) {
        // 非wal模式需要写入到空闲表
        FreeTablePending(&pager->free_table, pgid);
    }
}

/*
* 释放分配的页面
* 如果引用计数未清零则挂到待释放链表中(或循环等待)等待引用计数清空
*/
void PagerFree(Pager* pager, PageId pgid, bool skip_pool) {
    YuDb* db = ObjectGetFromField(pager, YuDb, pager);
    if (skip_pool == false && db->config.update_mode == kConfigUpdateWal) {
        // 仅wal模式启用空闲页号池
        PageIdVectorPushTail(&pager->free_pgid_pool, &pgid);
        return;
    }
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
	  assert(pgid != kPageInvalidId);
	CacheId cache_id = CacherFind(&pager->cacher, pgid, true);
	CacheInfo* info = CacherGetInfo(&pager->cacher, cache_id);
	void* cache;
	if (cache_id == kCacheInvalidId) {
		cache_id = CacherAlloc(&pager->cacher, pgid);
		if (cache_id == kCacheInvalidId) {
			  assert(0);
			return NULL;
		}
		cache = CacherGet(&pager->cacher, cache_id);

        if (!PagerRead(pager, pgid, cache, 1)) {
            // 读取失败直接返回空，因为被引用的页面要么在缓存中，要么已经落盘到磁盘
            CacherFree(&pager->cacher, cache_id);
            return NULL;
        }
    }
    else {
        cache = CacherGet(&pager->cacher, cache_id);
    }
      assert(cache);
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
* 清理页号池，待决、保留、空闲页面的实际释放
*/
void PagerCleanPageIdPool(Pager* pager) {
    YuDb* db = ObjectGetFromField(pager, YuDb, pager);

    // 等待所有读事务关闭
    while (db->tx_manager.read_tx_record.root != NULL) ThreadSwitch();

    // 将所有pending释放到空闲页池
    TxFreePendingPoolPage(db);

    for (int i = 0; i < pager->reserve_pgid_pool.count; i++) {
        PageId* pgid = PageIdVectorPopTail(&pager->reserve_pgid_pool);
        PagerFree(pager, pgid, false);
    }
    for (int i = 0; i < pager->free_pgid_pool.count; i++) {
        PageId* pgid = PageIdVectorPopTail(&pager->free_pgid_pool);
        PagerFree(pager, pgid, false);
    }
}

/*
* 同步落盘脏页队列所有脏页
*/
void PagerSyncWriteAllDirty(Pager* pager) {
    Cacher* cacher = &pager->cacher;
    YuDb* db = ObjectGetFromField(pager, YuDb, pager);
    CacheRbEntry* rb_entry = CacheRbTreeIteratorFirst(&cacher->dirty_tree);
    while (rb_entry) {
        CacheInfo* cache_info = ObjectGetFromField(rb_entry, CacheInfo, dirty_entry);
        rb_entry = CacheRbTreeIteratorNext(&cacher->dirty_tree, rb_entry);
        CacheRbTreeDelete(&cacher->dirty_tree, &cache_info->dirty_entry);
        // 写入1页
        CacheId dirty_cache_id = CacherGetIdByInfo(cacher, cache_info);
        void* cache = CacherGet(cacher, dirty_cache_id);
        CacheDoublyStaticListPush(cacher->cache_info_pool, kCacheTypeClean, dirty_cache_id);
        cache_info->type = kCacheTypeClean;
        PagerWrite(pager, cache_info->pgid, cache, 1);
        CacherDereference(cacher, dirty_cache_id);
    };
}