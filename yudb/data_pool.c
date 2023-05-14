#include "yudb/data_pool.h"


CUTILS_SPACE_MANAGER_OBJECT_POOL_DECLARATION(DataBlock, struct _DataBlockObjectPoolBucketEntry, PageId, int16_t)

#pragma pack(1)
typedef struct _DataBlockPool {
    DataBlockObjectPool pool;
    int16_t block_len;
} DataBlockPool;
#pragma pack()

inline static int16_t YUDB_DATA_BLOCK_POOL_ACCESSOR_GetMaxCount(DataBlockObjectPool* pool) {
    DataBlockPool* pool_ = (DataBlockPool*)pool;
    return pool_ ->block_len;
}
#define YUDB_DATA_BLOCK_POOL_ACCESSOR YUDB_DATA_BLOCK_POOL_ACCESSOR

inline static DataBlockObjectPoolBucketEntry* YUDB_DATA_BLOCK_POOL_INDEXER_GetPtr(DataBlockObjectPool* pool, DataBlockObjectPoolBucketEntry** arr, ptrdiff_t i) {
    DataBlockPool* pool_ = (DataBlockPool*)pool;
    return (DataBlockObjectPoolBucketEntry*)((uintptr_t)(*arr) + pool_->block_len * i);
}
#define YUDB_DATA_BLOCK_POOL_INDEXER YUDB_DATA_BLOCK_POOL_INDEXER

CUTILS_SPACE_MANAGER_OBJECT_POOL_DEFINE(DataBlock, DataBlockObjectPoolBucketEntry, PageId, int16_t, YUDB_DATA_BLOCK_POOL_ACCESSOR, CUTILS_OBJECT_ALLOCATOR_DEFALUT, CUTILS_OBJECT_REFERENCER_DEFALUT, YUDB_DATA_BLOCK_POOL_INDEXER)



void DataBlockPoolInit(DataBlockPool* data_pool, PageSize data_pool_size, uint16_t pool_select) {
    
}

PageOffset DataBlockPoolAlloc(DataBlockPool* data_pool) {
    
}

void DataBlockPoolFree(DataBlockPool* data_pool, PageOffset offset) {
    
}