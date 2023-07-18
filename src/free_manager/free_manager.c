#include <yudb/free_manager/free_manager.h>

#include <yudb/db_file.h>
#include <yudb/pager.h>
#include <yudb/yudb.h>

/*
* 获取空闲表对应PageId
*/
static PageId FreeManagerGetTablePageId(FreeManager* manager, void* table) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);
    YuDb* db = ObjectGetFromField(pager, YuDb, pager);

    PageId pgid;
    if (manager->free0_table == table) {
        pgid = kFreeTableStartId + db->meta_index;
    }
    else {
        CacheInfo* info = CacherGetInfo(&pager->cacher, CacherGetIdByBuf(&pager->cacher, table));
        pgid = info->pgid;
    }
    return pgid;
}

/*
* 空闲表标记为脏页
*/
static void FreeManagerMarkDirtyTable(FreeManager* table, void* free_table) {
    Pager* pager = ObjectGetFromField(table, Pager, free_manager);
    CacheId cache_id = CacherGetIdByBuf(&pager->cacher, free_table);
    CacheInfo* cache_info = CacherGetInfo(&pager->cacher, cache_id);
    CacherMarkDirty(&pager->cacher, cache_id);
}

/*
* 从空闲目录表获取子表
*/
static void* FreeManagerGetSubTable(FreeManager* manager, FreeDirTable* dir_table, PageOffset free_dir_entry_id, CacheId* cache_id) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);
    
    FreeDirEntry* table_entry = &FreeDirTableGetStaticList(dir_table)->obj_arr[free_dir_entry_id];

    // 根据dir_table的pgid拿到其sub_table
    PageId pgid = FreeManagerGetTablePageId(manager, dir_table);
    PageCount level = FreeTableGetLevel(pgid, pager->page_size);

    PageOffset page_table_max_count = FreePageTableGetMaxCount(pager->page_size);

    // 根据level来确定当前dir_table管理的页面数
    PageCount level_page_count = FreeTableGetLevelPageCount(level, pager->page_size);
    PageCount down_level_page_count = level_page_count / page_table_max_count;        // 获取其entry所管理的页面数

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
        sub_table_pgid_start = base + free_dir_entry_id * down_level_page_count;
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
            // 如果读取失败，若是从未使用过的free_table则将其构建
            if (base != kFreeTableStartId) {
                if (LIBYUC_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(table_entry->sub_max_free_log - 1) != down_level_page_count) {
                    return NULL;
                }
                FreeManagerBuildTable(manager, level + 1, (FreeDirTable*)write_cache);
            }
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
* 构建空闲表
*/
static bool FreeManagerBuildTable(FreeManager* manager, FreeLevel level, void* free_table) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);

    // CacheInfo* cache_info = CacherGetInfo(&pager->cacher, CacherGetIdByBuf(&pager->cacher, free_table));

    void* sub_table = free_table;
    CacheId cache_id = kCacheInvalidId;
    for (FreeLevel i = level; i < kFreeTableLevel - 1; i++) {
        FreeDirTableInit(sub_table, i, pager->page_size);
        // 当前dir_table页应该在page_table被分配
        // 第0项包括了dir_table自身，无法分配整个entry[0]，需要向下构建到page_table再根据level进行提前分配

        FreeDirStaticList* dir_static_list = FreeDirTableGetStaticList(sub_table);
        dir_static_list->obj_arr[0].sub_max_free_log -= 1;

        CacheId old_cache_id = cache_id;
        sub_table = FreeManagerGetSubTable(manager, sub_table, 0, &cache_id);


        if (old_cache_id != kCacheInvalidId) {
            CacherDereference(&pager->cacher, old_cache_id);
        }
    }
    FreePageTableInit(sub_table, pager->page_size);
    // dir_table和page_table所占用的页面都进行提前分配
    for (FreeLevel i = level; i < kFreeTableLevel; i++) {
        FreePageTableAlloc(sub_table, 2);
    }
    if (cache_id != kCacheInvalidId) {
        CacherDereference(&pager->cacher, cache_id);
    }
}

/*
* 递归分配
*/
static PageId FreeManagerAllocFromTable(FreeManager* manager, FreeLevel level, void* free_table, PageCount level_page_count, PageCount count) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);
    PageId pgid;
    PageOffset free_entry_id;
    if (level < kFreeTableLevel - 1) {
        PageOffset dir_max_page_count = FreeDirTableGetMaxCount(pager->page_size);
        PageOffset page_max_page_count = FreePageTableGetMaxCount(pager->page_size);
        level_page_count /= dir_max_page_count;
        if (level_page_count <= count) {
            // 在当前层进行分配
            for (FreeLevel i = 0; i < kFreeTableLevel - level - 1; i++) {
                count /= dir_max_page_count;
            }
            pgid = FreeManagerGetTablePageId(manager, free_table);
            pgid = pgid % page_max_page_count ? (pgid - pgid % page_max_page_count) : pgid;
            pgid += FreeDirTableAlloc(free_table, count, true) * level_page_count;
            return pgid;
        }
        CacheId cache_id;

        free_entry_id = FreeDirTableFindByPageCount(free_table, count);
        FreeDirStaticList* dir_static_list = FreeDirTableGetStaticList(free_table);
        FreeDirEntry* free_entry = &dir_static_list->obj_arr[free_entry_id];

        void* sub_table = FreeManagerGetSubTable(manager, free_table, free_entry_id, &cache_id);
        PageCount sub_max_free = LIBYUC_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(free_entry->sub_max_free_log - 1);
        if (sub_max_free == level_page_count) {
            // 需要构建
            FreeManagerBuildTable(manager, level + 1, sub_table);
        }

        pgid = FreeManagerAllocFromTable(manager, level + 1, sub_table, level_page_count, count);

        // 更新f1中对应的f2最大连续空位
        PageCount sub_free_page_count;
        if (level == kFreeTableLevel - 2) {
            sub_free_page_count = FreePageTableGetMaxFreeCount(sub_table) * level_page_count / page_max_page_count;
        }
        else {
            sub_free_page_count = FreeDirTableGetMaxFreeCount(sub_table) * level_page_count / dir_max_page_count;
        }
        free_entry->sub_max_free_log = LIBYUC_SPACE_MANAGER_BUDDY_TO_EXPONENT_OF_2(sub_free_page_count) + 1;
        if (free_entry->sub_max_free_log == 0) {
            // 下级没有可分配的空间，挂到满队列中
            // FreeDirStaticListSwitch(static_list, kFreeDirEntryListAlloc, free0_entry_prev_id, free0_entry_id, kFreeDirEntryListFull);
        }

        // 该sub表已是脏页
        if (free_entry->sub_table_dirty == false) {
            free_entry->sub_table_dirty = true;
            FreeManagerMarkDirtyTable(manager, sub_table);
        }
        CacherDereference(&pager->cacher, cache_id);
    }
    else {
        pgid = FreeManagerGetTablePageId(manager, free_table);
        pgid = pgid % level_page_count ? (pgid - pgid % level_page_count) : pgid;
        pgid += FreePageTableAlloc(free_table, count);
    }
    return pgid;
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
        // 构建页面
        // free0_table
        FreeManagerBuildTable(&db->pager.free_manager, 0, manager->free0_table);
        // 再分配2页，因为构建时meta_info的2页没有被计算
        FreeManagerAlloc(&db->pager.free_manager, 2);
    }
    return true;
}


/*
* 从空闲管理器中分配页面
*/
PageId FreeManagerAlloc(FreeManager* manager, PageCount count) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);
    PageCount level_page_count = FreeTableGetLevelPageCount(0, pager->page_size);
    return FreeManagerAllocFromTable(manager, 0, manager->free0_table, level_page_count, count);
}

/*
* 从空闲管理器中将页面置为待决状态
*/
void FreeManagerPending(FreeManager* manager, PageId pgid) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);
    FreeDirStaticList* static_list = FreeDirTableGetStaticList(manager->free0_table);
    int16_t free0_entry_id;
    int16_t free1_entry_id;
    //FreeManagerGetPosFromPageId(manager, pgid, &free0_entry_id, &free1_entry_id);

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
    //FreeManagerGetPosFromPageId(manager, pgid, &free0_entry_id, &free1_entry_id);
    FreeDirEntry* free0_entry = &static_list->obj_arr[free0_entry_id];

    CacheId cache_id;
    FreePageTable* free_page_table = FreeManagerGetSubTable(manager, manager->free0_table, free0_entry_id, &cache_id);
      assert(free_page_table != NULL);
    FreePageTableFree(free_page_table, free1_entry_id);
    FreeManagerMarkDirtyTable(manager, free_page_table);

    //FreeDirTableUpdateEntryFreeCount(free0_entry, free_page_table);

    free0_entry->sub_table_dirty = true;

    CacherDereference(&pager->cacher, cache_id);
}

/*
* 将空闲表中所有的pending页面释放
*/
void FreeManagerCleanPending(FreeManager* manager) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);
    FreeDirStaticList* f0_static_list = FreeDirTableGetStaticList(manager->free0_table);
    for (PageOffset i = 0; i < FreePageTableGetMaxCount(pager->page_size) - 3; i++) {
        FreeDirEntry* free0_entry = &f0_static_list->obj_arr[i];
        // 遍历f0_entry，将存在pending的f1清空的pending页面释放
        if (free0_entry->sub_table_pending == true) {
            CacheId cache_id;
            FreePageTable* free_page_table = FreeManagerGetSubTable(manager, manager->free0_table, i, &cache_id);
              assert(free_page_table != NULL);

            FreePageStaticList* f1_static_list = FreePageTableGetStaticList(free_page_table);
            PageOffset id = FreePageStaticListIteratorFirst(f1_static_list, kFreePageEntryListPending);
            while (id != YUDB_FREE_TABLE_REFERENCER_InvalidId) {
                FreePageTableFree(free_page_table, id + kFreePageStaticEntryIdOffset);
                id = FreePageStaticListIteratorNext(f1_static_list, id);
            }
            f1_static_list->list_first[kFreePageEntryListPending] = YUDB_FREE_TABLE_REFERENCER_InvalidId;

            free0_entry->sub_table_pending = false;
            CacherDereference(&pager->cacher, cache_id);

            //FreeDirTableUpdateEntryFreeCount(free0_entry, free_page_table);
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
        PageOffset free0_entry_id = FreeDirStaticListIteratorFirst(static_list, list_type[i]);
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
    PageId emm = FreeManagerAlloc(manager, 1);
    emm = FreeManagerAlloc(manager, 1);

    emm = FreeManagerAlloc(manager, 100);
    emm = FreeManagerAlloc(manager, 2048);
    emm = FreeManagerAlloc(manager, 2048);
    emm = FreeManagerAlloc(manager, 1024);
    emm = FreeManagerAlloc(manager, 2048);

    emm = FreeManagerAlloc(manager, 1024*1024);
}