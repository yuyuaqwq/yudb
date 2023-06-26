#include <yudb/data_pool.h>

#include <yudb/bucket.h>

#define YUDB_BUCKET_FREE_LIST_REFEREBCER_InvalidId (-1)
#define YUDB_BUCKET_FREE_LIST_REFEREBCER YUDB_BUCKET_FREE_LIST_REFEREBCER
CUTILS_CONTAINER_SPACE_MANAGER_FREE_LIST_DEFINE(DataPool, int16_t, uint8_t, YUDB_BUCKET_FREE_LIST_REFEREBCER, 1)

MemoryData g_key;
MemoryData g_value;

void DataPoolInit(DataPool* data_pool, size_t pool_size) {
	DataPoolFreeListInit(&data_pool->free_list, pool_size);
}
int16_t DataPoolAlloc(DataPool* data_pool, int16_t size) {
	BucketEntry* bucket_entry = ObjectGetFromField(ObjectGetFromField(data_pool, BucketEntryInfo, data_pool), BucketEntry, info);
	int16_t data_id = DataPoolFreeListAlloc(&data_pool->free_list, 0, &size);
	bucket_entry->info.alloc_size += size;
	return data_id;
}
void DataPoolRelease(DataPool* data_pool, int16_t data_id, size_t size) {
	BucketEntry* entry = ObjectGetFromField(ObjectGetFromField(data_pool, BucketEntryInfo, data_pool), BucketEntry, info);
	  assert(size >= sizeof(DataPoolFreeBlockEntry));
	  assert(entry->info.alloc_size >= 4 && entry->info.alloc_size <= entry->info.page_size);
	DataPoolFreeListFree(&data_pool->free_list, 0, data_id, &size);
	entry->info.alloc_size -= size;
}
void* DataPoolGetBlock(DataPool* data_pool, int16_t data_id) {
	return &data_pool->free_list.obj_arr[data_id];
}

void* DataDescriptorParser(DataPool* data_pool, DataDescriptor* data, size_t* size) {
	BucketEntry* bucket_entry = ObjectGetFromField(ObjectGetFromField(data_pool, BucketEntryInfo, data_pool), BucketEntry, info);
	void* data_buf = NULL;
	switch (data->type) {
	case kDataInline: {
		data_buf = data->inline_.data;
		*size = data->inline_.size;
		break;
	}
	case kDataBlock: {
		data_buf = (YuDbBPlusElement*)DataBlockGetBlock(data_pool, data->block.data_id);
		*size = data->block.size;
		break;
	case kDataMemory: {
		MemoryData* mem_data = data->mem_buf.is_value ? &g_value : &g_key;
		data_buf = mem_data->mem_ptr;
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
size_t DataDescriptorGetExpandSize(DataPool* data_pool, DataDescriptor* data) {
	if (data->type == kDataBlock) {
		return data->block.size;
	}
	else if (data->type == kDataMemory) {
		MemoryData* mem_data = data->mem_buf.is_value ? &g_value : &g_key;
		return mem_data->size;
	}
	return 0;
}
void DataDescriptorExpandRelease(DataPool* data_pool, DataDescriptor* data) {
	BucketEntry* bucket_entry = ObjectGetFromField(ObjectGetFromField(data_pool, BucketEntryInfo, data_pool), BucketEntry, info);
	if (data->type == kDataBlock) {
		DataPoolRelease(bucket_entry, data->block.data_id, data->block.size);
	}
}