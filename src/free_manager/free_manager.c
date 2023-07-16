#include <yudb/free_manager/free_manager.h>

#include <yudb/db_file.h>
#include <yudb/pager.h>
#include <yudb/yudb.h>


static void FreeManagerMarkDirtyTable(FreeManager* table, void* free_table) {
    Pager* pager = ObjectGetFromField(table, Pager, free_manager);
    CacheId cache_id = CacherGetIdByBuf(&pager->cacher, free_table);
    CacheInfo* cache_info = CacherGetInfo(&pager->cacher, cache_id);
    CacherMarkDirty(&pager->cacher, cache_id);
}

static void* FreeManagerGetSubTable(FreeManager* manager, FreeDirTable* dir_table, PageOffset free_dir_entry_id, CacheId* cache_id) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);
    YuDb* db = ObjectGetFromField(pager, YuDb, pager);

    FreeDirEntry* table_entry = &FreeDirTableGetStaticList(dir_table)->obj_arr[free_dir_entry_id];

    // 根据dir_table的pgid拿到其sub_table
    PageId pgid;
    if (manager->free0_table == dir_table) {
        pgid = kFreeTableStartId + db->meta_index;
    }
    else {
        CacheInfo* info = CacherGetInfo(&pager->cacher, CacherGetIdByBuf(&pager->cacher, dir_table));
        pgid = info->pgid;
    }
    uint32_t level = FreeTableGetLevel(pgid, pager->page_size);

    int16_t page_table_max_count = FreePageTableGetMaxCount(pager->page_size);

    // 根据level来确定当前dir_table管理的页面数
    uint32_t page_count = FreeTableGetPageCount(level, pager->page_size);
    page_count /= page_table_max_count;        // 获取其entry所管理的页面数

    PageId base = pgid % page_table_max_count ? (pgid - pgid % page_table_max_count) : pgid;

    if (base == kMetaStartId) {
        base = kFreeTableStartId;
    }
    PageId sub_table_pgid_start;
    // 其孩子，除了free_dir_entry_id为0时是挨着当前pgid之外，都是对齐到page_count边界的
    if (free_dir_entry_id == 0) {
        sub_table_pgid_start = (pgid + 2) & (PageId)-2;
    }
    else {
        sub_table_pgid_start = base + free_dir_entry_id * page_count;
    }

    // 从一端读取(最新版本)，写入到另一端(旧版本)
    PageId sub_table_pgid_read = sub_table_pgid_start + table_entry->read_select;
    PageId sub_table_pgid_write = sub_table_pgid_start + table_entry->write_select;

    FreePageEntry* read_cache, * write_cache;
    CacheId read_cache_id = CacherFind(&pager->cacher, sub_table_pgid_read, true);
    CacheId write_cache_id;
    if (sub_table_pgid_read != sub_table_pgid_write) {
        write_cache_id = CacherFind(&pager->cacher, sub_table_pgid_write, true);
    }
    else {
        write_cache_id = read_cache_id;
    }
    if (write_cache_id == kCacheInvalidId) {
        write_cache_id = CacherAlloc(&pager->cacher, sub_table_pgid_write);
    }
    write_cache = (FreePageEntry*)CacherGet(&pager->cacher, write_cache_id);

    if (read_cache_id == kCacheInvalidId) {
        if (!PagerRead(pager, sub_table_pgid_read, write_cache, 1)) {
            // 如果读取失败，若是从未使用过的free_table则将其初始化
            if (level == kFreeTableLevel - 1) {
                if (LIBYUC_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(table_entry->sub_max_free_log - 1) != FreePageTableGetMaxCount(pager->page_size)) {
                    return NULL;
                }
            } else {
                if (LIBYUC_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(table_entry->sub_max_free_log - 1) != FreeDirTableGetMaxCount(pager->page_size)) {
                    return NULL;
                }
            }
            FreeManagerBuildTable((FreeDirTable*)write_cache, level + 1, pager->page_size);
        }
    }
    else {
        if (sub_table_pgid_read != sub_table_pgid_write) {
            read_cache = (FreePageEntry*)CacherGet(&pager->cacher, read_cache_id);
            memcpy(write_cache, read_cache, pager->page_size);
            CacherDereference(&pager->cacher, read_cache_id);
        }
    }

    if (table_entry->read_select != table_entry->write_select) {
        // 初次从磁盘读取之后，由于read的内容会被拷贝到write，并且write可能会被修改，此时write才是最新的版本(尚未落盘)，下次read应该读取当前的write
        table_entry->read_select = table_entry->write_select;
    }
    if (cache_id) { *cache_id = write_cache_id; }
    return write_cache;
}


/*
* 初始化空闲表
*/
bool FreeManagerInit(FreeManager* manager) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);
    YuDb* db = ObjectGetFromField(pager, YuDb, pager);

    int16_t max_count = FreeDirTableGetMaxCount(pager->page_size);
    manager->free0_table = (FreeDirTable*)malloc(pager->page_size);

    // free0_table常驻内存
    PageId free_dir_table_pgid = kFreeTableStartId + db->meta_index;
    if (!PagerRead(pager, free_dir_table_pgid, db->pager.free_manager.free0_table, 1)) {
        return false;
    }
    return true;
}

/*
* 递归分配，返回的PageId基于free_table的PageId的
*/
PageId FreeManagerAllocFromTable(FreeManager* manager, uint32_t level, void* free_table, uint32_t cur_level_max_count, uint32_t count) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);
    PageOffset free_entry_id;
    if (level < kFreeTableLevel - 1) {
        PageOffset dir_max_count = FreeDirTableGetMaxCount(pager->page_size);
        cur_level_max_count /= dir_max_count;
        if (cur_level_max_count < count) {
            // 在当前层进行分配
            for (int32_t i = 0; i < kFreeTableLevel - level - 1; i++) {
                count /= dir_max_count;
            }
            FreeDirTableAlloc(free_table, count);
            return;
        }
        CacheId cache_id;
        
        free_entry_id = FreeDirTableFindByPageCount(free_table, count);
        
        void* sub_table = FreeManagerGetSubTable(manager, free_table, free_entry_id, &cache_id);
        FreeDirStaticList* static_list = FreeDirTableGetStaticList(sub_table);
        uint32_t sub_max_free = LIBYUC_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(static_list->obj_arr[free_entry_id].sub_max_free_log - 1);
        if (sub_max_free == cur_level_max_count) {
            // 需要构建
            FreeManagerBuildTable((FreeDirTable*)sub_table, level + 1, pager->page_size);
        }

        FreeManagerAllocFromTable(manager, level + 1, sub_table, cur_level_max_count, count);

        FreeDirEntry* free_entry = &static_list->obj_arr[free_entry_id];

        // 更新f1中对应的f2最大连续空位
        free_entry->sub_max_free_log = LIBYUC_SPACE_MANAGER_BUDDY_TO_EXPONENT_OF_2(FreePageTableGetMaxFreeCount(sub_table)) + 1;
        if (free_entry->sub_max_free_log == 0) {
            // 下级没有可分配的空间，挂到满队列中
            // FreeDirStaticListSwitch(static_list, kFreeDirEntryListAlloc, free0_entry_prev_id, free0_entry_id, kFreeDirEntryListFull);
        }

        // 该sub表已是脏页
        free_entry->sub_table_dirty = true;
        FreeManagerMarkDirtyTable(manager, sub_table);

        CacherDereference(&pager->cacher, cache_id);
    }
    else {
        FreePageTableAlloc(free_table, count);
    }
}

bool FreeManagerBuildTable(FreeManager* manager, uint32_t level, void* free_table) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);

    CacheInfo* cache_info = CacherGetInfo(&pager->cacher, CacherGetIdByBuf(&pager->cacher, free_table));

    void* sub_table = free_table;
    CacheId cache_id = kCacheInvalidId;
    for (int32_t i = level; i < kFreeTableLevel - 2; i++) {
        FreeDirTableInit(sub_table, i, pager->page_size);
        CacheId old_cache_id = cache_id;
        sub_table = FreeManagerGetSubTable(manager, sub_table, 0, &cache_id);
        if (old_cache_id != kCacheInvalidId) {
            CacherDereference(&pager->cacher, old_cache_id);
        }
    }
    FreePageTableInit(sub_table, pager->page_size);
    // dir_table和page_table所占用的页面都进行提前分配
    for (int32_t i = level; i < kFreeTableLevel - 1; i++) {
        FreePageTableAlloc(sub_table, 2);
    }
    if (cache_id != kCacheInvalidId) {
        CacherDereference(&pager->cacher, cache_id);
    }
}

/*
* 从空闲管理器中分配页面，返回f2_id
*/

/*
* 是当前层级的数量级，就调用当前层级的BuddyAlloc，返回
* 不是当前层级的数量级，调用findsub，从当前SubAlloc静态链中查找足够分配的entry
*    如果没有，就调用BuddyAlloc分配一个entry，挂到SubAlloc，并且初始化entry对应的sub_table(实际上需要循环到page_table，在该page_table中分配n*2页面, n是循环的次数)
*    进入该sub_table，递归
*    更新entry的sub_max_free
*/
PageId FreeManagerAlloc(FreeManager* manager, int32_t count) {

    FreeManagerAllocFromTable(manager, 0, manager->free0_table, , count);

}

/*
* 从空闲管理器中将页面置为待决状态
*/
void FreeManagerPending(FreeManager* manager, PageId pgid) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);
    FreeDirStaticList* static_list = FreeDirTableGetStaticList(manager->free0_table);
    int16_t free0_entry_id;
    int16_t free1_entry_id;
    FreeManagerGetPosFromPageId(manager, pgid, &free0_entry_id, &free1_entry_id);

    CacheId cache_id;
    FreePageEntry* free_page_table = FreeManagerGetSubTable(manager, manager->free0_table, free0_entry_id, &cache_id);
    FreePageTablePending(free_page_table, free1_entry_id);
    FreeManagerMarkDirtyTable(manager, free_page_table);

    FreeDirEntry* free0_entry = &static_list->obj_arr[free0_entry_id];
    free0_entry->sub_table_dirty = true;
    free0_entry->sub_table_pending = true;

    CacherDereference(&pager->cacher, cache_id);
}

/*
* 从空闲表中释放页面
*/
void FreeManagerFree(FreeManager* manager, PageId pgid) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);
    FreeDirStaticList* static_list = FreeDirTableGetStaticList(manager->free0_table);

    int16_t free0_entry_id;
    int16_t free1_entry_id;
    FreeManagerGetPosFromPageId(manager, pgid, &free0_entry_id, &free1_entry_id);
    FreeDirEntry* free0_entry = &static_list->obj_arr[free0_entry_id];

    CacheId cache_id;
    FreePageTable* free_page_table = FreeManagerGetSubTable(manager, manager->free0_table, free0_entry_id, &cache_id);
      assert(free_page_table != NULL);
    FreePageTableFree(free_page_table, free1_entry_id);
    FreeManagerMarkDirtyTable(manager, free_page_table);

    FreeDirTableUpdateEntryFreeCount(free0_entry, free_page_table);

    free0_entry->sub_table_dirty = true;

    CacherDereference(&pager->cacher, cache_id);
}

/*
* 将空闲表中所有的pending页面释放
*/
void FreeManagerCleanPending(FreeManager* manager) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);
    FreeDirStaticList* f0_static_list = FreeDirTableGetStaticList(manager->free0_table);
    for (int16_t i = 0; i < FreePageTableGetMaxCount(pager->page_size) - 3; i++) {
        FreeDirEntry* free0_entry = &f0_static_list->obj_arr[i];
        // 遍历f0_entry，将存在pending的f1清空的pending页面释放
        if (free0_entry->sub_table_pending == true) {
            CacheId cache_id;
            FreePageTable* free_page_table = FreeManagerGetSubTable(manager, manager->free0_table, i, &cache_id);
              assert(free_page_table != NULL);

            FreePageStaticList* f1_static_list = FreePageTableGetStaticList(free_page_table);
            int16_t id = FreePageStaticListIteratorFirst(f1_static_list, kFreePageEntryListPending);
            while (id != YUDB_FREE_TABLE_REFERENCER_InvalidId) {
                FreePageTableFree(free_page_table, id + kFreePageStaticEntryIdOffset);
                id = FreePageStaticListIteratorNext(f1_static_list, id);
            }
            f1_static_list->list_first[kFreePageEntryListPending] = YUDB_FREE_TABLE_REFERENCER_InvalidId;

            free0_entry->sub_table_pending = false;
            CacherDereference(&pager->cacher, cache_id);

            FreeDirTableUpdateEntryFreeCount(free0_entry, free_page_table);
        }
    }
}

/*
* 空闲管理器持久化
*/
bool FreeManagerWrite(FreeManager* manager, int32_t meta_index) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);
    YuDb* db = ObjectGetFromField(pager, YuDb, pager);
    PageId pgid = (kFreeTableStartId + meta_index);

    bool dirty = false;
    FreeDirEntryListType list_type[] = { kFreeDirEntryListSubAlloc, /*kFreeDirEntryListFull*/ };
    FreeDirStaticList* static_list = FreeDirTableGetStaticList(manager->free0_table);
    for (int i = 0; i < sizeof(list_type) / sizeof(FreeDirEntryListType); i++) {
        int16_t free0_entry_id = FreeDirStaticListIteratorFirst(static_list, list_type[i]);
        while (free0_entry_id != YUDB_FREE_TABLE_REFERENCER_InvalidId) {
            FreeDirEntry* free0_entry = &static_list->obj_arr[free0_entry_id];
            // 有f1脏页的话，f0需要更新对应的select
            if (free0_entry->sub_table_dirty == true) {
                // 落盘时read和write是相同的，write切到另一侧
                  assert(free0_entry->write_select == free0_entry->read_select);
                free0_entry->write_select = (free0_entry->read_select + 1) % 2;
                free0_entry->sub_table_dirty = false;
                dirty = true;
            }
            free0_entry_id = FreeDirStaticListIteratorNext(static_list, free0_entry_id);
        }
    }
    if (dirty) {
        return PagerWrite(pager, pgid, manager->free0_table, 1);
    }
    return true;
}




void FreeManagerTest(FreeManager* manager) {
    int16_t free0_entry_id_out, free1_entry_id_out;
    int16_t emm = FreeManagerAlloc(manager, 1, &free0_entry_id_out, &free1_entry_id_out);
    emm = FreeManagerAlloc(manager, 1, &free0_entry_id_out, &free1_entry_id_out);

    emm = FreeManagerAlloc(manager, 100, &free0_entry_id_out, &free1_entry_id_out);
    emm = FreeManagerAlloc(manager, 2048, &free0_entry_id_out, &free1_entry_id_out);
    emm = FreeManagerAlloc(manager, 2048, &free0_entry_id_out, &free1_entry_id_out);
}