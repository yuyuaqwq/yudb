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

static void* FreeManagerGetSubTable(FreeManager* manager, FreeDirTable* dir_table, int16_t free_dir_entry_id, CacheId* cache_id) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);
    YuDb* db = ObjectGetFromField(pager, YuDb, pager);

    FreeDirEntry* dir_entry = &FreeDirTableGetStaticList(dir_table)->obj_arr[free_dir_entry_id];

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
    PageId sub_table_pgid_read = sub_table_pgid_start + dir_entry->read_select;
    PageId sub_table_pgid_write = sub_table_pgid_start + dir_entry->write_select;

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
                if (LIBYUC_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(dir_entry->sub_max_free_log - 1) != FreePageTableGetMaxCount(pager->page_size)) {
                    return NULL;
                }
                FreePageTableInit((FreePageTable*)write_cache, pager->page_size);
            }
            else {
                if (LIBYUC_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(dir_entry->sub_max_free_log - 1) != FreeDirTableGetMaxCount(pager->page_size)) {
                    return NULL;
                }
                FreeDirTableInit((FreeDirTable*)write_cache, pager->page_size, level + 1);
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

    if (dir_entry->read_select != dir_entry->write_select) {
        // 初次从磁盘读取之后，由于read的内容会被拷贝到write，并且write可能会被修改，此时write才是最新的版本(尚未落盘)，下次read应该读取当前的write
        dir_entry->read_select = dir_entry->write_select;
    }
    if (cache_id) { *cache_id = write_cache_id; }
    return write_cache;
}

PageId FreeManagerPosToPageId(FreeManager* manager, int16_t free0_entry_id, int16_t free1_entry_pos) {
    return free0_entry_id * FreePageTableGetMaxCount(FreeDirGetPageSize(manager->free0_table)) + free1_entry_pos;
}

void FreeManagerGetPosFromPageId(FreeManager* manager, PageId pgid, int16_t* free0_entry_id, int16_t* free1_entry_id) {
    *free0_entry_id = pgid / FreePageTableGetMaxCount(FreeDirGetPageSize(manager->free0_table));
    *free1_entry_id = pgid % FreePageTableGetMaxCount(FreeDirGetPageSize(manager->free0_table));
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
* 从空闲管理器中分配页面，返回f2_id
*/

/*
* 是当前层级的数量级，就调用当前层级的BuddyAlloc，返回
* 不是当前层级的数量级，调用findsub，从当前SubAlloc静态链中查找足够分配的entry
*    如果没有，就调用BuddyAlloc分配一个entry，挂到SubAlloc，并且初始化entry对应的sub_table
*    进入该sub_table，递归
*    更新entry的sub_max_free
*/
int16_t FreeManagerAlloc(FreeManager* manager, int32_t count, int16_t* free0_entry_id_out, int16_t* free1_entry_id_out) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);

    if (!LIBYUC_SPACE_MANAGER_BUDDY_IS_POWER_OF_2(count)) {
        count = LIBYUC_SPACE_MANAGER_BUDDY_ALIGN_TO_POWER_OF_2(count);
    }

    int16_t page_table_max_count = FreePageTableGetMaxCount(pager->page_size);
    int16_t dir_table_max_count = FreeDirTableGetMaxCount(pager->page_size);
    
    uint32_t max_page_count = FreeTableGetPageCount(0, pager->page_size);
    if (count > max_page_count) {
        return YUDB_FREE_TABLE_REFERENCER_InvalidId;
    }

    // 根据分配大小从不同层级开始分配

    FreeDirStaticList* f0_static_list = FreeDirTableGetStaticList(manager->free0_table);

    if (count >= dir_table_max_count * page_table_max_count) {
        // 直接从f0分配
        int16_t f0_count = count / (dir_table_max_count * page_table_max_count);
        f0_count += count % (dir_table_max_count * page_table_max_count) ? 1 : 0;
        int16_t free0_entry_id = FreeDirTableAlloc(manager->free0_table, f0_count);
        if (free0_entry_id == YUDB_FREE_TABLE_REFERENCER_InvalidId) {
            return YUDB_FREE_TABLE_REFERENCER_InvalidId;
        }
        FreeDirEntry* free0_entry = &f0_static_list->obj_arr[free0_entry_id];
        FreeDirStaticListPush(f0_static_list, kFreeDirEntryListSubAlloc, free0_entry_id);
        // free0_entry->entry_list_type = kFreeDirEntryListAlloc;

        *free0_entry_id_out = free0_entry_id;
        *free1_entry_id_out = 0;
        return 0;// free0_entry_id* dir_table_max_count* page_table_max_count;
    }

    // 从f0中查找足够空位的f0_entry
    int16_t free0_entry_id = FreeDirTableFindBySubPageCount(manager->free0_table, count);
    if (free0_entry_id == YUDB_FREE_TABLE_REFERENCER_InvalidId) {
        return YUDB_FREE_TABLE_REFERENCER_InvalidId;
    }
    *free0_entry_id_out = free0_entry_id;
    FreeDirEntry* free0_entry = &f0_static_list->obj_arr[free0_entry_id];

    CacheId f1_cache_id;
    FreeDirTable* free1_table = (FreeDirTable*)FreeManagerGetSubTable(manager, manager->free0_table, free0_entry_id, &f1_cache_id);
    FreeDirStaticList* f1_static_list = FreeDirTableGetStaticList(free1_table);

    int16_t free1_entry_id;
    if (count >= page_table_max_count) {
        // 从f1分配页面
          assert(free1_table != NULL);
        uint32_t sub_max_free = LIBYUC_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(f0_static_list->obj_arr[free0_entry_id].sub_max_free_log - 1);
        if (sub_max_free == page_table_max_count * dir_table_max_count) {
            // 初次从f0中分配的f1，应当构建f2的关系，前2页应当是被分配的，但此时还未构建f3，顺便构建f3并从f3分配前4页？
            FreeDirTableAlloc(free1_table, kFreePageStaticEntryIdOffset);
        }

        int16_t f1_count = count / page_table_max_count;
        f1_count += count % page_table_max_count ? 1 : 0;

        free1_entry_id = FreeDirTableAlloc(free1_table, f1_count);
        if (free1_entry_id == YUDB_FREE_TABLE_REFERENCER_InvalidId) {
            return YUDB_FREE_TABLE_REFERENCER_InvalidId;
        }
        FreeDirEntry* free1_entry = &f1_static_list->obj_arr[free1_entry_id];
        FreeDirStaticListPush(f1_static_list, kFreeDirEntryListSubAlloc, free1_entry_id);

        free0_entry->sub_max_free_log = LIBYUC_SPACE_MANAGER_BUDDY_TO_EXPONENT_OF_2(FreeDirTableGetMaxFreeCount(free1_table) * page_table_max_count) + 1;
        if (free0_entry->sub_max_free_log == 0) {
            // 下级没有可分配的空间，挂到满队列中
            // FreeDirStaticListSwitch(static_list, kFreeDirEntryListAlloc, free0_entry_prev_id, free0_entry_id, kFreeDirEntryListFull);
        }

        // 该f1表已是脏页
        free0_entry->sub_table_dirty = true;
        FreeManagerMarkDirtyTable(manager, free1_table);

        CacherDereference(&pager->cacher, f1_cache_id);

        *free1_entry_id_out = free1_entry_id;
        return 0;// free0_entry_id* dir_table_max_count* page_table_max_count + free1_entry_id * page_table_max_count;
    }

    free1_entry_id = FreeDirTableFindBySubPageCount(free1_table, count);
    if (free1_entry_id == YUDB_FREE_TABLE_REFERENCER_InvalidId) {
        return YUDB_FREE_TABLE_REFERENCER_InvalidId;
    }
    *free1_entry_id_out = free1_entry_id;

    

    // 从f2分配页面
    CacheId f2_cache_id;
    FreePageTable* free_page_table = FreeManagerGetSubTable(manager, free1_table, free1_entry_id, &f2_cache_id);
    int16_t free2_entry_id = YUDB_FREE_TABLE_REFERENCER_InvalidId;
    do {
          assert(free_page_table != NULL);
        if (LIBYUC_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(f1_static_list->obj_arr[free1_entry_id].sub_max_free_log - 1) == page_table_max_count) {
            // 初次从f1中分配的f2，前2页提前占用
            FreePageTableAlloc(free_page_table, kFreePageStaticEntryIdOffset);
        }
        free2_entry_id = FreePageTableAlloc(free_page_table, count);
        if (free2_entry_id == YUDB_FREE_TABLE_REFERENCER_InvalidId) {
            break;
        }

        FreeDirEntry* free1_entry = &f1_static_list->obj_arr[free1_entry_id];

        // 更新f1中对应的f2最大连续空位
        free1_entry->sub_max_free_log = LIBYUC_SPACE_MANAGER_BUDDY_TO_EXPONENT_OF_2(FreePageTableGetMaxFreeCount(free_page_table)) + 1;
        if (free1_entry->sub_max_free_log == 0) {
            // 下级没有可分配的空间，挂到满队列中
            // FreeDirStaticListSwitch(static_list, kFreeDirEntryListAlloc, free0_entry_prev_id, free0_entry_id, kFreeDirEntryListFull);
        }

        // 该f2表已是脏页
        free0_entry->sub_table_dirty = true;
        free1_entry->sub_table_dirty = true;
        FreeManagerMarkDirtyTable(manager, free1_table);
        FreeManagerMarkDirtyTable(manager, free_page_table);

    } while (false);
    

    CacherDereference(&pager->cacher, f1_cache_id);
    CacherDereference(&pager->cacher, f2_cache_id);
      assert(free2_entry_id);
    return free2_entry_id;
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