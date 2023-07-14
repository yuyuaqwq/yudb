#include <yudb/free_manager/free_table.h>

const PageId kMetaStartId = 0;
const PageId kFreeTableStartId = 2;
const uint32_t kFreeTableLevel = 3;

LIBYUC_SPACE_MANAGER_BUDDY_DEFINE(FreeTable, int16_t, LIBYUC_SPACE_MANAGER_BUDDY_4BIT_INDEXER, LIBYUC_OBJECT_ALLOCATOR_DEFALUT)


/*
* 获取不同层级的空闲表所管理的page_count
*/
uint32_t FreeTableGetPageCount(uint32_t level, int16_t page_size) {
    uint32_t page_count = FreePageTableGetMaxCount(page_size);
    for (uint32_t i = 1; i < kFreeTableLevel - level; i++) {
        page_count *= FreeDirTableGetMaxCount(page_size);
    }
    return page_count;
}

uint32_t FreeTableGetLevel(PageId pgid, int16_t page_size) {
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
    if (pgid < kFreeTableStartId + kFreeTableLevel * 2) {
        return (pgid - kFreeTableStartId) / 2;
    }

    pgid &= ((PageId)-2);

    int16_t page_table_max_count = FreePageTableGetMaxCount(page_size);
    PageId level_pgid_factor = FreeTableGetPageCount(0, page_size);

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