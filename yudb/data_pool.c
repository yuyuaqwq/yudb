#include "yudb/data_pool.h"

const PageSize kDataPoolElementSize[] = { 16, 32, 64, 128, 256, 512, 1024 };
#define kDataPoolCount sizeof(kDataPoolElementSize) / sizeof(PageSize)
// const uint16_t kDataPoolCount = sizeof(kDataPoolElementSize) / sizeof(PageSize);

typedef struct _DataPool {
    StaticList list;
    PageId next_data_pool;      // 连接属于同一页面(节点)的所有相同尺寸的数据池
} DataPool;

typedef struct _DataEntry {
    StaticListEntry entry;
} DataEntry;

void DataPoolInit(DataPool* data_pool, PageSize data_pool_size, uint16_t pool_select) {
    StaticListInitFromBuf(&data_pool->list, data_pool, data_pool_size / kDataPoolElementSize[pool_select], kDataPoolElementSize[pool_select], 0, 1);
}

PageOffset DataPoolAlloc(DataPool* data_pool) {
    uint16_t index = StaticListPop(&data_pool->list, 0);
    PageOffset offset = index * ArrayGetObjectSize(&data_pool->list.array);
    return offset;
}

void DataPoolFree(DataPool* data_pool, PageOffset offset) {
    uint16_t index = offset / ArrayGetObjectSize(&data_pool->list.array);
    StaticListPush(&data_pool->list, 0, index);
}