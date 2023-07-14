#include <yudb/free_manager/free_manager.h>

#include <yudb/db_file.h>
#include <yudb/pager.h>
#include <yudb/yudb.h>


LIBYUC_SPACE_MANAGER_BUDDY_DEFINE(Free, int16_t, LIBYUC_SPACE_MANAGER_BUDDY_4BIT_INDEXER, LIBYUC_OBJECT_ALLOCATOR_DEFALUT)


#define YUDB_FREE_TABLE_REFERENCER_InvalidId (-1)
#define YUDB_FREE_TABLE_REFERENCER YUDB_FREE_TABLE_REFERENCER
#define YUDB_FREE_TABLE_ACCESSOR_GetNext(list, element) ((element)->entry_list_next)
#define YUDB_FREE_TABLE_ACCESSOR_SetNext(list, element, new_next) ((element)->entry_list_next = new_next)
#define YUDB_FREE_TABLE_ACCESSOR YUDB_FREE_TABLE_ACCESSOR
LIBYUC_CONTAINER_STATIC_LIST_DEFINE(FreeDir, int16_t, FreeDirEntry, YUDB_FREE_TABLE_REFERENCER, YUDB_FREE_TABLE_ACCESSOR, 4)

LIBYUC_CONTAINER_STATIC_LIST_DEFINE(FreePage, int16_t, FreePageEntry, YUDB_FREE_TABLE_REFERENCER, YUDB_FREE_TABLE_ACCESSOR, 2)


const PageId kMetaStartId = 0;
const PageId kFreeDirTableStartId = 2;

const uint16_t kFreeDirUnableToManageEntryCount = 3;
const uint16_t kFreePageStaticEntryIdOffset = 2;

const uint32_t kFreeTableLevel = 3;



static int16_t FreePageTableGetMaxCount(int16_t page_size) {
    return (page_size - (page_size / 4)) / sizeof(FreePageEntry);
}

static int16_t FreeDirTableGetMaxCount(int16_t page_size) {
    return (page_size - (page_size / 4)) / sizeof(FreeDirEntry);
}

static void FreeDirOrPageTableMarkDirty(FreeManager* table, void* free_table) {
    Pager* pager = ObjectGetFromField(table, Pager, free_manager);
    CacheId cache_id = CacherGetIdByBuf(&pager->cacher, free_table);
    CacheInfo* cache_info = CacherGetInfo(&pager->cacher, cache_id);
    CacherMarkDirty(&pager->cacher, cache_id);
}

/*
* 获取不同层级的空闲表所管理的page_count
*/
static uint32_t FreeDirOrPageTableGetPageCount(uint32_t level, int16_t page_size) {
    uint32_t page_count = FreePageTableGetMaxCount(page_size);
    for (uint32_t i = 1; i < kFreeTableLevel - level; i++) {
        page_count *= FreeDirTableGetMaxCount(page_size);
    }
    return page_count;
}

static uint32_t FreeDirOrPageTableGetLevel(PageId pgid, int16_t page_size) {
    /*
    * 2(0) 4(1) 6(2)
    * 1024(2)
    * 2048
    * 3072
    * ...
    * 1048576(1) 1048578(2)
    * 1049600(2)
    * ...
    * 2097152(1) 2097154(2)
    * ...
    */
    if (pgid < kFreeDirTableStartId + kFreeTableLevel * 2) {
        return (pgid - kFreeDirTableStartId) / 2;
    }

    pgid &= ((PageId)-2);

    int16_t page_table_max_count = FreePageTableGetMaxCount(page_size);
    PageId level_pgid_factor = FreeDirOrPageTableGetPageCount(0, page_size);

    // 先对齐到page_count边界
    PageId offset = pgid % page_table_max_count;
    PageId base = offset ? (pgid - offset) : pgid;

    // base可能落到1024、1048576、...页面，如果是1024则必定是level2，如果是1048576则可能是level1，也可能是level2
    uint32_t level = 1;
    for (; level < kFreeTableLevel - 1; level++) {
        if (base % level_pgid_factor == 0) {
            break;
        }
        level_pgid_factor /= FreeDirTableGetMaxCount(page_size);
    }

    // 根据offset算出最终level
    offset /= 2;

    level = level + offset;

    return level;
}



/*
* FreePageTable
*/


static int16_t FreePageGetPageSize(FreePageTable* free_page_table) {
    return FreeBuddyGetMaxCount(&free_page_table->buddy) * 4;
}


FreePageStaticList* FreePageTableGetStaticList(FreePageTable* free_page_table) {
    return (FreePageStaticList*)((uintptr_t)free_page_table + FreeBuddyGetMaxCount(&free_page_table->buddy));
}

int16_t FreePageTableGetMaxFreeCount(FreePageTable* free_page_table) {
    return FreeBuddyGetMaxFreeCount(&free_page_table->buddy);
}

void FreePageTableInit(FreePageTable* free_page_table, int16_t page_size) {
    int16_t max_count = FreePageTableGetMaxCount(page_size);
    FreeBuddyInit(&free_page_table->buddy, max_count);
    max_count -= kFreePageStaticEntryIdOffset;
    FreePageStaticListInit(FreePageTableGetStaticList(free_page_table), max_count);
}

int16_t FreePageTableAlloc(FreePageTable* free_page_table, int16_t count) {
    return FreeBuddyAlloc(&free_page_table->buddy, count);
}

void FreePageTablePending(FreePageTable* free_page_table, int16_t free1_entry_id) {
      assert(free1_entry_id != -1);
    FreePageStaticList* static_list = FreePageTableGetStaticList(free_page_table);
    FreePageEntry* free1_entry = &static_list->obj_arr[free1_entry_id - kFreePageStaticEntryIdOffset];
    free1_entry->is_pending = true;
    FreePageStaticListPush(static_list, kFreePageEntryListPending, free1_entry_id - kFreePageStaticEntryIdOffset);
}

void FreePageTableFree(FreePageTable* free_page_table, int16_t free1_entry_id) {
      assert(free1_entry_id != -1);
    FreePageStaticList* static_list = FreePageTableGetStaticList(free_page_table);
    FreePageEntry* free1_entry = &static_list->obj_arr[free1_entry_id - kFreePageStaticEntryIdOffset];
    if (free1_entry->is_pending) {
        free1_entry->is_pending = false;
        // 将其从Pending链表中摘除
        int16_t cur_id = FreePageStaticListIteratorFirst(static_list, kFreePageEntryListPending);
        int16_t prev_id = YUDB_FREE_TABLE_REFERENCER_InvalidId;
        while (cur_id != YUDB_FREE_TABLE_REFERENCER_InvalidId) {
            if (cur_id == free1_entry_id - kFreePageStaticEntryIdOffset) {
                FreePageStaticListDelete(static_list, kFreePageEntryListPending, prev_id, cur_id);
                break;
            }
            prev_id = cur_id;
            cur_id = FreePageStaticListIteratorNext(static_list, cur_id);
        }
    }
    FreeBuddyFree(&free_page_table->buddy, free1_entry_id);
}



/*
* FreeDirTable
*/

static int16_t FreeDirTableGetMaxFreeCount(FreeDirTable* free_dir_table) {
    return FreeBuddyGetMaxFreeCount(&free_dir_table->buddy);
}

static int16_t FreeDirGetPageSize(FreeDirTable* free_dir_table) {
    return FreeBuddyGetMaxCount(&free_dir_table->buddy) * 4;
}

FreeDirStaticList* FreeDirTableGetStaticList(FreeDirTable* free_dir_table) {
    return (FreeDirStaticList*)((uintptr_t)free_dir_table + FreeBuddyGetMaxCount(&free_dir_table->buddy));
}

void FreeDirTableInit(FreeDirTable* free_dir_table, int16_t page_size, int32_t level) {
    int16_t max_count = FreeDirTableGetMaxCount(page_size);
    FreeBuddyInit(&free_dir_table->buddy, max_count);
    max_count -= kFreeDirUnableToManageEntryCount;
    FreeDirStaticList* static_list = FreeDirTableGetStaticList(free_dir_table);
    FreeDirStaticListInit(static_list, max_count);
    
    uint32_t sub_max_free_page = FreeDirOrPageTableGetPageCount(level, page_size);

    for (uint32_t i = 0; i < max_count; i++) {
        uint32_t aa = LIBYUC_SPACE_MANAGER_BUDDY_TO_EXPONENT_OF_2(sub_max_free_page);
        static_list->obj_arr[i].sub_max_free_log = LIBYUC_SPACE_MANAGER_BUDDY_TO_EXPONENT_OF_2(sub_max_free_page) + 1;
        static_list->obj_arr[i].read_select = 1;
        static_list->obj_arr[i].write_select = 0;
        static_list->obj_arr[i].sub_table_pending = false;
    }
    // 自身？
    static_list->obj_arr[0].sub_max_free_log -= 1;
}

int16_t FreeDirTableAlloc(FreeDirTable* dir_table, int16_t count) {
    return FreeBuddyAlloc(&dir_table->buddy, count);
}

void FreeDirTableFree(FreeDirTable* dir_table, int16_t dir_entry_id) {
    FreeBuddyFree(&dir_table->buddy, dir_entry_id);
}

/*
* 从dir_table中查找足够空位的sub_table
*/
int16_t FreeDirTableFindBySubFreeCount(FreeDirTable* dir_table, int32_t sub_count) {
    int16_t dir_entry_prev_id = YUDB_FREE_TABLE_REFERENCER_InvalidId;
    FreeDirStaticList* static_list = FreeDirTableGetStaticList(dir_table);
    int16_t dir_entry_id = FreeDirStaticListIteratorFirst(static_list, kFreeDirEntryListAlloc);
    while (true) {
        FreeDirEntry* free_dir_entry;
        if (dir_entry_id == YUDB_FREE_TABLE_REFERENCER_InvalidId) {
            dir_entry_id = FreeDirTableAlloc(dir_table, 1);
            if (dir_entry_id == YUDB_FREE_TABLE_REFERENCER_InvalidId) {
                return YUDB_FREE_TABLE_REFERENCER_InvalidId;
            }
            FreeDirStaticListPush(static_list, kFreeDirEntryListAlloc, dir_entry_id);
            // static_list->obj_arr[dir_entry_id].entry_list_type = kFreeDirEntryListAlloc;
        }
        free_dir_entry = &static_list->obj_arr[dir_entry_id];
        int32_t sub_max_free = LIBYUC_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(free_dir_entry->sub_max_free_log-1);
        if (sub_max_free >= sub_count) {
            break;
        }
        dir_entry_prev_id = dir_entry_id;
        dir_entry_id = FreeDirStaticListIteratorNext(static_list, dir_entry_id);
    }
    return dir_entry_id;
}

void* FreeDirTableGetSubTable(FreeManager* manager, FreeDirTable* dir_table, int16_t free_dir_entry_id, CacheId* cache_id) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);
    YuDb* db = ObjectGetFromField(pager, YuDb, pager);

    FreeDirEntry* dir_entry = &FreeDirTableGetStaticList(dir_table)->obj_arr[free_dir_entry_id];

    // 根据dir_table的pgid拿到其sub_table
    PageId pgid;
    if (manager->free0_table == dir_table) {
        pgid = kFreeDirTableStartId + db->meta_index;
    }
    else {
        CacheInfo* info = CacherGetInfo(&pager->cacher, CacherGetIdByBuf(&pager->cacher, dir_table));
        pgid = info->pgid;
    }
    uint32_t level = FreeDirOrPageTableGetLevel(pgid, pager->page_size);

    int16_t page_table_max_count = FreePageTableGetMaxCount(pager->page_size);

    // 根据level来确定当前dir_table管理的页面数
    uint32_t page_count = FreeDirOrPageTableGetPageCount(level, pager->page_size);
    page_count /= page_table_max_count;        // 获取其entry所管理的页面数

    PageId base = pgid % page_table_max_count ? (pgid - pgid % page_table_max_count) : pgid;
    
    if (base == kMetaStartId) {
        base = kFreeDirTableStartId;
    }
    PageId sub_table_pgid_start;
    // 其孩子，除了free_dir_entry_id为0时是挨着当前pgid之外，都是对齐到page_count边界的
    if (free_dir_entry_id == 0){
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
    } else {
        write_cache_id = read_cache_id;
    }
    if (write_cache_id == kCacheInvalidId) {
        write_cache_id = CacherAlloc(&pager->cacher, sub_table_pgid_write);
    }
    write_cache = (FreePageEntry*)CacherGet(&pager->cacher, write_cache_id);

    if (read_cache_id == kCacheInvalidId) {
        if (!PagerRead(pager, sub_table_pgid_read, write_cache, 1)) {
            // 如果读取失败，若是从未使用过的free_table则将其初始化
            if (LIBYUC_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(dir_entry->sub_max_free_log-1) != FreePageTableGetMaxCount(pager->page_size)) {
                return NULL;
            }
            if (level == kFreeTableLevel - 1) {
                FreePageTableInit((FreePageTable*)write_cache, pager->page_size);
            }
            else {
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

void FreeDirTableUpdateEntryFreeCount(FreeDirEntry* free_dir_entry, FreePageTable* free_page_table) {
    free_dir_entry->sub_max_free_log = LIBYUC_SPACE_MANAGER_BUDDY_TO_EXPONENT_OF_2(FreePageTableGetMaxFreeCount(free_page_table)) + 1;
    if (free_dir_entry->sub_max_free_log != 0) {
        // 挂回可分配队列
        //FreeDirStaticListSwitch(static_list, kFreeDirEntryListFull, free0_entry_prev_id, free0_entry_id, kFreeDirEntryListAlloc);
    }
}





/*
* FreeManager
*/
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
    PageId free_dir_table_pgid = kFreeDirTableStartId + db->meta_index;
    if (!PagerRead(pager, free_dir_table_pgid, db->pager.free_manager.free0_table, 1)) {
        return false;
    }
    return true;
}

/*
* 从空闲管理器中分配页面，返回f2_id
*/
int16_t FreeManagerAlloc(FreeManager* manager, int32_t count, int16_t* free0_entry_id_out, int16_t* free1_entry_id_out) {
    Pager* pager = ObjectGetFromField(manager, Pager, free_manager);

    if (!LIBYUC_SPACE_MANAGER_BUDDY_IS_POWER_OF_2(count)) {
        count = LIBYUC_SPACE_MANAGER_BUDDY_ALIGN_TO_POWER_OF_2(count);
    }

    int16_t page_table_max_count = FreePageTableGetMaxCount(pager->page_size);
    int16_t dir_table_max_count = FreeDirTableGetMaxCount(pager->page_size);
    
    uint32_t max_page_count = FreeDirOrPageTableGetPageCount(0, pager->page_size);
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
        FreeDirStaticListPush(f0_static_list, kFreeDirEntryListAlloc, free0_entry_id);
        // free0_entry->entry_list_type = kFreeDirEntryListAlloc;

        *free0_entry_id_out = free0_entry_id;
        *free1_entry_id_out = 0;
        return 0;// free0_entry_id* dir_table_max_count* page_table_max_count;
    }


    // 从f0中查找足够空位的f0_entry
    int16_t free0_entry_id = FreeDirTableFindBySubFreeCount(manager->free0_table, count);
    if (free0_entry_id == YUDB_FREE_TABLE_REFERENCER_InvalidId) {
        return YUDB_FREE_TABLE_REFERENCER_InvalidId;
    }
    *free0_entry_id_out = free0_entry_id;

    // 从f1分配
    

    FreeDirEntry* free0_entry = &f0_static_list->obj_arr[free0_entry_id];

    CacheId f1_cache_id;
    FreeDirTable* free1_table = (FreeDirTable*)FreeDirTableGetSubTable(manager, manager->free0_table, free0_entry_id, &f1_cache_id);

    int16_t free1_entry_id;
    if (count >= page_table_max_count) {
        // 从f1分配页面
        int16_t f1_count = count / page_table_max_count;
        f1_count += count % page_table_max_count ? 1 : 0;

        free1_entry_id = FreeDirTableAlloc(manager->free0_table, f1_count);
        if (free1_entry_id == YUDB_FREE_TABLE_REFERENCER_InvalidId) {
            return YUDB_FREE_TABLE_REFERENCER_InvalidId;
        }

        
        FreeDirStaticList* f1_static_list = FreeDirTableGetStaticList(free1_table);
        FreeDirEntry* free1_entry = &f1_static_list->obj_arr[free1_entry_id];
        FreeDirStaticListPush(f1_static_list, kFreeDirEntryListAlloc, free1_entry_id);


        free0_entry->sub_max_free_log = LIBYUC_SPACE_MANAGER_BUDDY_TO_EXPONENT_OF_2(FreeDirTableGetMaxFreeCount(free1_table) * page_table_max_count) + 1;
        if (free0_entry->sub_max_free_log == 0) {
            // 下级没有可分配的空间，挂到满队列中
            // FreeDirStaticListSwitch(static_list, kFreeDirEntryListAlloc, free0_entry_prev_id, free0_entry_id, kFreeDirEntryListFull);
        }

        // 该f1表已是脏页
        free0_entry->sub_table_dirty = true;
        if (free0_entry_id_out) {
            *free0_entry_id_out = free0_entry_id;
        }
        FreeDirOrPageTableMarkDirty(manager, free1_table);


        CacherDereference(&pager->cacher, f1_cache_id);

        *free1_entry_id_out = free1_entry_id;
        return 0;// free0_entry_id* dir_table_max_count* page_table_max_count + free1_entry_id * page_table_max_count;
    }

    free1_entry_id = FreeDirTableFindBySubFreeCount(free1_table, count);
    if (free1_entry_id == YUDB_FREE_TABLE_REFERENCER_InvalidId) {
        return YUDB_FREE_TABLE_REFERENCER_InvalidId;
    }

    // 从f2分配页面
    CacheId f2_cache_id;
    FreePageTable* free_page_table = FreeDirTableGetSubTable(manager, free1_table, free1_entry_id, &f2_cache_id);
    int16_t free2_entry_id = YUDB_FREE_TABLE_REFERENCER_InvalidId;
    do {
        assert(free_page_table != NULL);
        if (LIBYUC_SPACE_MANAGER_BUDDY_TO_POWER_OF_2(f0_static_list->obj_arr[free0_entry_id].sub_max_free_log - 1) == page_table_max_count) {
            // 初次分配的f2，前2页提前占用
            FreePageTableAlloc(free_page_table, kFreePageStaticEntryIdOffset);
        }
        free2_entry_id = FreePageTableAlloc(free_page_table, count);
        if (free2_entry_id == YUDB_FREE_TABLE_REFERENCER_InvalidId) {
            break;
        }

        // 更新f0中对应的f1最大连续空闲
        free0_entry->sub_max_free_log = LIBYUC_SPACE_MANAGER_BUDDY_TO_EXPONENT_OF_2(FreePageTableGetMaxFreeCount(free_page_table)) + 1;
        if (free0_entry->sub_max_free_log == 0) {
            // 下级没有可分配的空间，挂到满队列中
            // FreeDirStaticListSwitch(static_list, kFreeDirEntryListAlloc, free0_entry_prev_id, free0_entry_id, kFreeDirEntryListFull);
        }

        // 该f2表已是脏页
        free0_entry->sub_table_dirty = true;
        if (free0_entry_id_out) {
            *free0_entry_id_out = free0_entry_id;
        }
        FreeDirOrPageTableMarkDirty(manager, free_page_table);
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
    FreePageEntry* free_page_table = FreeDirTableGetSubTable(manager, manager->free0_table, free0_entry_id, &cache_id);
    FreePageTablePending(free_page_table, free1_entry_id);
    FreeDirOrPageTableMarkDirty(manager, free_page_table);

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
    FreePageTable* free_page_table = FreeDirTableGetSubTable(manager, manager->free0_table, free0_entry_id, &cache_id);
      assert(free_page_table != NULL);
    FreePageTableFree(free_page_table, free1_entry_id);
    FreeDirOrPageTableMarkDirty(manager, free_page_table);

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
            FreePageTable* free_page_table = FreeDirTableGetSubTable(manager, manager->free0_table, i, &cache_id);
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
    PageId pgid = (kFreeDirTableStartId + meta_index);

    bool dirty = false;
    FreeDirEntryListType list_type[] = { kFreeDirEntryListAlloc, /*kFreeDirEntryListFull*/ };
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
    emm = FreeManagerAlloc(manager, 1024, &free0_entry_id_out, &free1_entry_id_out);
}