#ifndef YUDB_FREE_MANAGER_FREE_PAGE_TABLE_H_
#define YUDB_FREE_MANAGER_FREE_PAGE_TABLE_H_

#include <stdbool.h>
#include <stdint.h>

#include <yudb/page.h>
#include <yudb/free_manager/free_table.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

typedef enum {
    //kFreePageEntryListFree = 0,
    kFreePageEntryListPending = 1,
} FreePageEntryListType;

#pragma pack(1)
typedef struct _FreePageEntry {
    int16_t entry_list_next;
    struct {
        uint8_t is_pending : 1;
        uint8_t : 7;
    };
} FreePageEntry;
#pragma pack()
LIBYUC_CONTAINER_STATIC_LIST_DECLARATION(FreePage, int16_t, FreePageEntry, 2)

typedef struct _FreePageTable {
    FreeTableBuddy buddy;
} FreePageTable;


void FreePageTableInit(FreePageTable* page_table, int16_t page_size);
int16_t FreePageTableAlloc(FreePageTable* page_table, int16_t count);
int16_t FreePageTableGetMaxFreeCount(FreePageTable* page_table);
FreePageStaticList* FreePageTableGetStaticList(FreePageTable* page_table);

extern const uint16_t kFreePageStaticEntryIdOffset;

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_FREE_MANAGER_FREE_PAGE_TABLE_H_