#include <yudb/free_manager/free_table.h>

const PageId kMetaStartId = 0;
const PageId kFreeTableStartId = 2;
const PageCount kFreeTableLevel = 3;

LIBYUC_SPACE_MANAGER_BUDDY_DEFINE(FreeTable, PageOffset, LIBYUC_SPACE_MANAGER_BUDDY_4BIT_INDEXER, LIBYUC_OBJECT_ALLOCATOR_DEFALUT)


/*
* 获取不同层级的空闲表所管理的page_count
*/
PageCount FreeTableGetLevelPageCount(FreeLevel level, PageOffset page_size) {
    PageCount page_count = FreePageTableGetMaxCount(page_size);
    for (PageCount i = 1; i < kFreeTableLevel - level; i++) {
        page_count *= FreeDirTableGetMaxCount(page_size);
    }
    return page_count;
}