#ifndef YUDB_FREE_MANAGER_FREE_TABLE_H_
#define YUDB_FREE_MANAGER_FREE_TABLE_H_

#include <stdbool.h>
#include <stdint.h>

#include <libyuc/space_manager/buddy.h>
#include <libyuc/container/static_list.h>

#include <yudb/page.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

typedef enum {
    kFreeDirTable = 0,
    kFreePageTable = 1,
} FreeTableType;

LIBYUC_SPACE_MANAGER_BUDDY_DECLARATION(FreeTable, PageOffset)

#define YUDB_FREE_TABLE_REFERENCER_InvalidId (-1)
#define YUDB_FREE_TABLE_REFERENCER YUDB_FREE_TABLE_REFERENCER
#define YUDB_FREE_TABLE_ACCESSOR_GetNext(list, element) ((element)->entry_list_next)
#define YUDB_FREE_TABLE_ACCESSOR_SetNext(list, element, new_next) ((element)->entry_list_next = new_next)
#define YUDB_FREE_TABLE_ACCESSOR YUDB_FREE_TABLE_ACCESSOR

extern const PageId kMetaStartId;
extern const PageId kFreeTableStartId;
extern const uint32_t kFreeTableLevel;

PageCount FreeTableGetPageCount(uint32_t level, PageOffset page_size);
PageCount FreeTableGetLevel(PageId pgid, PageOffset page_size);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_FREE_MANAGER_FREE_TABLE_H_