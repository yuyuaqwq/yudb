#include <yudb/data_pool.h>

#include <yudb/bucket.h>

#define LIBYUC_SPACE_MANAGER_FREE_LIST_CLASS_NAME DataPool
#include <libyuc/space_manager/free_list.c>

//#define YUDB_BUCKET_FREE_LIST_REFEREBCER_InvalidId (-1)
//#define YUDB_BUCKET_FREE_LIST_REFEREBCER YUDB_BUCKET_FREE_LIST_REFEREBCER
//LIBYUC_CONTAINER_SPACE_MANAGER_FREE_LIST_DEFINE(DataPool, int16_t, uint8_t, YUDB_BUCKET_FREE_LIST_REFEREBCER, 1)

MemoryData g_key;
MemoryData g_value;

void DataPoolInit(DataPool* data_pool, size_t pool_size) {
    DataPoolFreeListInit(&data_pool->free_list, pool_size);
}

int16_t DataPoolAlloc(DataPool* data_pool, int16_t size) {
    BucketEntry* bucket_entry = ObjectGetFromField(
        ObjectGetFromField(data_pool, BucketEntryInfo, data_pool), 
        BucketEntry, info
    );
    int16_t data_id = DataPoolFreeListAlloc(&data_pool->free_list, 0, &size);
    bucket_entry->info.alloc_size += size;
    int16_t free_size;
      assert(bucket_entry->info.alloc_size + (free_size = DataPoolFreeListGetFreeBlockSize(&bucket_entry->info.data_pool.free_list, 0)) == bucket_entry->info.page_size - 2);
    return data_id;
}

void DataPoolRelease(DataPool* data_pool, int16_t data_id, int16_t size) {
    BucketEntry* bucket_entry = ObjectGetFromField(
        ObjectGetFromField(data_pool, BucketEntryInfo, data_pool), 
        BucketEntry, info
    );
      assert(size >= sizeof(DataPoolFreeBlockEntry));
      assert(bucket_entry->info.alloc_size >= 4 && bucket_entry->info.alloc_size <= bucket_entry->info.page_size);
    DataPoolFreeListFree(&data_pool->free_list, 0, data_id, &size);
    bucket_entry->info.alloc_size -= size;
    int16_t free_size;
      assert(bucket_entry->info.alloc_size + (free_size = DataPoolFreeListGetFreeBlockSize(&bucket_entry->info.data_pool.free_list, 0)) == bucket_entry->info.page_size - 2);
}

void* DataPoolGetBlock(DataPool* data_pool, int16_t data_id) {
    return &data_pool->free_list.obj_arr[data_id];
}


void* DataDescriptorParser(DataPool* data_pool, DataDescriptor* data, int16_t* size) {
    BucketEntry* bucket_entry = ObjectGetFromField(ObjectGetFromField(data_pool, BucketEntryInfo, data_pool), BucketEntry, info);
    void* data_buf = NULL;
    switch (data->type) {
    case kDataInline: {
        data_buf = data->inline_.data;
        *size = data->inline_.size;
        break;
    }
    case kDataBlock: {
        data_buf = (YuDbBPlusElement*)DataPoolGetBlock(data_pool, data->block.data_id);
        *size = data->block.size;
        break;
    case kDataMemory: {
        MemoryData* mem_data = data->mem_buf.is_value ? &g_value : &g_key;
        data_buf = (void*)mem_data->mem_ptr;
        *size = mem_data->size;
        break;
    }
    }
    default:
        *size = 0;
        break;
    }
    return data_buf;
}

int16_t DataDescriptorGetExpandSize(DataPool* data_pool, DataDescriptor* data) {
    if (data->type == kDataBlock) {
        return data->block.size;
    }
    else if (data->type == kDataMemory) {
        MemoryData* mem_data = data->mem_buf.is_value ? &g_value : &g_key;
        return mem_data->size;
    }
    return 0;
}

void DataDescriptorReleaseExpand(DataPool* data_pool, DataDescriptor* data) {
    BucketEntry* bucket_entry = ObjectGetFromField(ObjectGetFromField(data_pool, BucketEntryInfo, data_pool), BucketEntry, info);
    if (data->type == kDataBlock) {
        DataPoolRelease(&bucket_entry->info.data_pool, data->block.data_id, data->block.size);
    }
}