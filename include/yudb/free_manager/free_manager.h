#ifndef YUDB_FREE_MANAGER_H_
#define YUDB_FREE_MANAGER_H_

#include <stdbool.h>
#include <stdint.h>

#include <libyuc/container/vector.h>
#include <libyuc/container/static_list.h>

#include <yudb/page.h>
#include <yudb/free_manager/free_dir_table.h>
#include <yudb/free_manager/free_page_table.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus


/*
* 当前的单向静态链表队列切换较困难，故暂时不分Full队列
*/

typedef struct _FreeManager {
    FreeDirTable* free0_table;
} FreeManager;


bool FreeManagerInit(FreeManager* free_manager);
bool FreeManagerBuildTable(FreeManager* manager, FreeLevel level, void* free_table);
PageId FreeManagerAlloc(FreeManager* free_manager, PageCount count);
void FreeManagerPending(FreeManager* free_manager, PageId pgid);
void FreeManagerFree(FreeManager* free_manager, PageId pgid);
void FreeManagerCleanPending(FreeManager* free_manager);
bool FreeManagerWrite(FreeManager* free_manager, int32_t meta_index);



void FreeManagerTest(FreeManager* manager);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_FREE_MANAGER_H_

