#include <yudb/bucket.h>

#include <yudb/yudb.h>
#include <yudb/pager.h>

#define YUDB_BUCKET_FREE_LIST_REFEREBCER_InvalidId (-1)
#define YUDB_BUCKET_FREE_LIST_REFEREBCER YUDB_BUCKET_FREE_LIST_REFEREBCER
//CUTILS_CONTAINER_SPACE_MANAGER_FREE_LIST_DEFINE(YuDbBPlusEntry, int16_t, uint8_t, YUDB_BUCKET_FREE_LIST_REFEREBCER, 1)
void YuDbBPlusEntryFreeListInit(YuDbBPlusEntryFreeList* head, int16_t element_count) {
	head->first_block[0] = 0;
	for (int16_t i = 1; i < 1; i++) {
		head->first_block[i] = (-1);
	}
	YuDbBPlusEntryFreeBlockEntry* block = (YuDbBPlusEntryFreeBlockEntry*)(&head->obj_arr[0]);
	block->next_block_offset = (-1);
	block->count = element_count - element_count % sizeof(YuDbBPlusEntryFreeBlockEntry);
}
int16_t YuDbBPlusEntryFreeListAlloc(YuDbBPlusEntryFreeList* head, int16_t list_order, int16_t* count) {
	if (*count % sizeof(YuDbBPlusEntryFreeBlockEntry)) {
		*count = *count + (sizeof(YuDbBPlusEntryFreeBlockEntry) - *count % sizeof(YuDbBPlusEntryFreeBlockEntry));
	}
	int16_t count_ = *count;
	YuDbBPlusEntryFreeBlockEntry* prev_block = (YuDbBPlusEntryFreeBlockEntry*)(&head->first_block[list_order]);
	int16_t free_offset = head->first_block[list_order];
	while (free_offset != (-1)) {
		YuDbBPlusEntryFreeBlockEntry* block = (YuDbBPlusEntryFreeBlockEntry*)(&head->obj_arr[free_offset]);
		if (block->count > count_) {
			YuDbBPlusEntryFreeBlockEntry* new_block = (YuDbBPlusEntryFreeBlockEntry*)(&head->obj_arr[free_offset + count_]);
			int16_t new_count = block->count - count_;
			new_block->next_block_offset = block->next_block_offset;
			  assert(new_block->next_block_offset != 0);
			new_block->count = block->count - count_;
			prev_block->next_block_offset += count_;
			return free_offset;
		}
		else if (block->count == count_) {
			prev_block->next_block_offset = block->next_block_offset;
			return free_offset;
		}
		free_offset = block->next_block_offset;
		prev_block = block;
	}
	;
	return (-1);
}
void YuDbBPlusEntryFreeListFree(YuDbBPlusEntryFreeList* head, int16_t list_order, int16_t free_offset, int16_t* count) {
	if (*count % sizeof(YuDbBPlusEntryFreeBlockEntry)) {
		*count = *count + (sizeof(YuDbBPlusEntryFreeBlockEntry) - *count % sizeof(YuDbBPlusEntryFreeBlockEntry));
	}
	int16_t count_ = *count;
	int16_t cur_offset = head->first_block[list_order];
	YuDbBPlusEntryFreeBlockEntry* prev_block = (YuDbBPlusEntryFreeBlockEntry*)(&head->first_block[list_order]);
	YuDbBPlusEntryFreeBlockEntry* cur_block;
	YuDbBPlusEntryFreeBlockEntry* free_prev_prev_block = ((void*)0), * free_next_prev_block = ((void*)0);
	YuDbBPlusEntryFreeBlockEntry* free_prev_block = ((void*)0), * free_next_block = ((void*)0);
	while (cur_offset != (-1)) {
		cur_block = (YuDbBPlusEntryFreeBlockEntry*)(&head->obj_arr[cur_offset]);
		if (!free_next_block && free_offset + count_ == cur_offset) {
			if (free_prev_block) {
				free_prev_prev_block->next_block_offset = free_prev_block->next_block_offset;
			}
			count_ += cur_block->count;
			int16_t next_offset = cur_block->next_block_offset;
			cur_block = (YuDbBPlusEntryFreeBlockEntry*)(&head->obj_arr[free_offset]);
			cur_block->count = count_;
			cur_block->next_block_offset = next_offset;
			prev_block->next_block_offset = free_offset;
			free_next_prev_block = prev_block;
			free_next_block = cur_block;
		}
		else if (!free_prev_block && cur_offset + cur_block->count == free_offset) {
			if (free_next_block) {
				free_next_prev_block->next_block_offset = free_next_block->next_block_offset;
			}
			free_offset = cur_offset;
			count_ += cur_block->count;
			cur_block->count = count_;
			free_prev_prev_block = prev_block;
			free_prev_block = cur_block;
		}
		else {
			prev_block = cur_block;
		}
		if (free_prev_block && free_next_block) break;
		cur_offset = cur_block->next_block_offset;
	}
	if (!free_prev_block && !free_next_block) {
		cur_block = (YuDbBPlusEntryFreeBlockEntry*)(&head->obj_arr[free_offset]);
		cur_block->next_block_offset = head->first_block[list_order];
		cur_block->count = count_;
		head->first_block[list_order] = free_offset;
	}
}
int16_t YuDbBPlusEntryFreeListGetMaxFreeBlockSize(YuDbBPlusEntryFreeList* head, int16_t list_order) {
	YuDbBPlusEntryFreeBlockEntry* prev_block = (YuDbBPlusEntryFreeBlockEntry*)((uintptr_t)&head->first_block[list_order]);
	int16_t free_offset = head->first_block[list_order];
	int16_t max = 0;
	while (free_offset != (-1)) {
		YuDbBPlusEntryFreeBlockEntry* block = (YuDbBPlusEntryFreeBlockEntry*)(&head->obj_arr[free_offset]);
		if (block->count > max) {
			max = block->count;
		}
		free_offset = block->next_block_offset;
	}
	return max;
}
int16_t YuDbBPlusEntryFreeListGetFreeBlockSize(YuDbBPlusEntryFreeList* head, int16_t list_order) {
	YuDbBPlusEntryFreeBlockEntry* prev_block = (YuDbBPlusEntryFreeBlockEntry*)((uintptr_t)&head->first_block[list_order]);
	int16_t free_offset = head->first_block[list_order];
	int16_t max = 0;
	while (free_offset != (-1)) {
		YuDbBPlusEntryFreeBlockEntry* block = (YuDbBPlusEntryFreeBlockEntry*)(&head->obj_arr[free_offset]);
		max += block->count;
		free_offset = block->next_block_offset;
	}
	return max;
}

static inline Tx* BPlusTreeToTx(YuDbBPlusTree* tree) {
	Bucket* bucket = ObjectGetFromField(tree, Bucket, bp_tree);
	MetaInfo* meta_info = ObjectGetFromField(bucket, MetaInfo, bucket);
	Tx* tx = ObjectGetFromField(meta_info, Tx, meta_info);
	return tx;
}

static inline uint32_t BucketEntryGetHeadSize(BucketEntry* entry) {
	return sizeof(BucketEntryInfo);
}

static inline uint32_t BPlusEntryGetHeadSize(BucketEntry* entry) {
	return sizeof(BucketEntryInfo);
}


static inline YuDbBPlusEntry* BucketEntryToBPlusEntry(BucketEntry* entry) {
	return (YuDbBPlusEntry*)((uintptr_t)entry + BucketEntryGetHeadSize(entry));
}

static inline BucketEntry* BPlusEntryToBucketEntry(YuDbBPlusEntry* entry) {
	return (BucketEntry*)((uintptr_t)entry - BPlusEntryGetHeadSize(entry));
}


/*
* B+树Entry分配器
*/
static void BucketEntryInit(BucketEntry* entry, size_t bp_entry_size, size_t page_size) {
	YuDbBPlusEntryFreeListInit(&entry->info.free_list, page_size - BucketEntryGetHeadSize(entry));
	
	// BPlusEntry头部占用
	entry->info.page_size = page_size;
	YuDbBPlusEntryFreeListAlloc(&entry->info.free_list, 0, &bp_entry_size);
	entry->info.alloc_size = BucketEntryGetHeadSize(entry) + bp_entry_size;		// 分配大小把BucketEntry头部长度算上，而free_list分配的偏移是不算上的
}

PageId YUDB_BUCKET_BPLUS_ENTRY_ALLOCATOR_CreateBySize(YuDbBPlusTree* tree, size_t bp_entry_size) {
	Tx* tx = BPlusTreeToTx(tree);
	PageId pgid = PagerAlloc(&tx->db->pager, true, 1);
	BucketEntry* entry = (BucketEntry*)PagerReference(&tx->db->pager, pgid);
	PagerMarkDirty(&tx->db->pager, entry);
	
	BucketEntryInit(entry, bp_entry_size, tx->db->pager.page_size);

	entry->info.last_write_tx_id = tx->meta_info.txid;
	PagerDereference(&tx->db->pager, entry);
	return pgid;
}
void YUDB_BUCKET_BPLUS_ENTRY_ALLOCATOR_Release(YuDbBPlusTree* tree, PageId pgid) {
	Tx* tx = BPlusTreeToTx(tree);
	PagerPending(&tx->db->pager, tx, pgid);
}
#define YUDB_BUCKET_BPLUS_ENTRY_ALLOCATOR YUDB_BUCKET_BPLUS_ENTRY_ALLOCATOR

/*
* B+树Entry引用器
*/
#define YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_InvalidId -1
YuDbBPlusEntry* YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(YuDbBPlusTree* tree, PageId pgid) {
	Tx* tx = BPlusTreeToTx(tree);
	BucketEntry* entry = (BucketEntry*)PagerReference(&tx->db->pager, pgid);
	return BucketEntryToBPlusEntry(entry);
}
void YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(YuDbBPlusTree* tree, YuDbBPlusEntry* bp_entry) {
	Tx* tx = BPlusTreeToTx(tree);
	BucketEntry* entry = BPlusEntryToBucketEntry(bp_entry);
	PagerDereference(&tx->db->pager, entry);
}
#define YUDB_BUCKET_BPLUS_ENTRY_REFERENCER YUDB_BUCKET_BPLUS_ENTRY_REFERENCER

/*
* B+树Entry访问器
*/
int32_t YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR_GetMergeThresholdRate(YuDbBPlusTree* tree, YuDbBPlusEntry* entry) {
	Tx* tx = BPlusTreeToTx(tree);
	return tx->db->pager.page_size * 40 / 100;
}
int32_t YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR_GetFreeRate(YuDbBPlusTree* tree, YuDbBPlusEntry* entry) {
	Tx* tx = BPlusTreeToTx(tree); 
	BucketEntry* bucket_entry = BPlusEntryToBucketEntry(entry);
	int16_t max_free_size = YuDbBPlusEntryFreeListGetMaxFreeBlockSize(&bucket_entry->info.free_list, 0);
	  assert(max_free_size <= bucket_entry->info.page_size - bucket_entry->info.alloc_size);
	return max_free_size;
}
int32_t YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR_GetMaxRate(YuDbBPlusTree* tree, YuDbBPlusEntry* entry) {
	Tx* tx = BPlusTreeToTx(tree);
	return tx->db->pager.page_size;
}
int32_t YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR_GetFillRate(YuDbBPlusTree* tree, YuDbBPlusEntry* entry) {
	Tx* tx = BPlusTreeToTx(tree);
	BucketEntry* bucket_entry = BPlusEntryToBucketEntry(entry);
	return bucket_entry->info.alloc_size;
}
YuDbBPlusEntry* YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR_GetTempCopyEntry(YuDbBPlusTree* tree, YuDbBPlusEntry* bp_entry) {
	Tx* tx = BPlusTreeToTx(tree);
	BucketEntry* temp_entry = tx->db->pager.temp_page;
	YuDbBPlusEntry* temp_bp_entry = BucketEntryToBPlusEntry(temp_entry);
	BucketEntry* entry = BPlusEntryToBucketEntry(bp_entry);
	memcpy(temp_entry, entry, tx->db->pager.page_size);
	return temp_bp_entry;
}
void YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR_Clean(YuDbBPlusTree* tree, YuDbBPlusEntry* bp_entry) {
	Tx* tx = BPlusTreeToTx(tree);
	uint16_t old_type = bp_entry->type;
	BucketEntry* entry = BPlusEntryToBucketEntry(bp_entry);
	BucketEntryInit(entry, bp_entry->type == kBPlusEntryLeaf ? sizeof(YuDbBPlusLeafElement) : sizeof(YuDbBPlusIndexElement), tx->db->pager.page_size);
	bp_entry->type = old_type;
	bp_entry->element_count = 0;
	YuDbBPlusEntryRbTreeInit(&bp_entry->rb_tree);
}
#define YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR


/*
* B+树Element引用器
*/
#define YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_InvalidId (-1)
forceinline YuDbBPlusElement* YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(YuDbBPlusEntry* entry, int16_t element_id) {
	return (YuDbBPlusElement*)((uintptr_t)entry + element_id);
}
forceinline void YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(YuDbBPlusEntry* entry, YuDbBPlusElement* element) {

}
#define YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER


MemoryData g_key;
MemoryData g_value;
forceinline void BlockRelease(BucketEntry* entry, int16_t element_id, size_t size) {
	  assert(size >= sizeof(YuDbBPlusEntryFreeBlockEntry));
	  assert(entry->info.alloc_size >= 4 && entry->info.alloc_size <= 4092);
	YuDbBPlusEntryFreeListFree(&entry->info.free_list, 0, element_id, &size);
	entry->info.alloc_size -= size;
}
forceinline void* DataParser(YuDbBPlusEntry* entry, Data* data, size_t* size) {
	void* data_buf = NULL;
	if (data->type == kDataInline) {
		data_buf = data->inline_.data;
		*size = data->inline_.size;
	}
	else if (data->type == kDataBlock) {
		data_buf = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(entry, data->block.element_id);
		*size = data->block.size;
	}
	else if (data->type == kDataMemory) {
		MemoryData* mem_data = data->mem_buf.is_value ? &g_value : &g_key;
		data_buf = mem_data->mem_ptr;
		*size = mem_data->size;
	}
	else {
		*size = 0;
	}
	return data_buf;
}
forceinline size_t DataGetExpandSize(YuDbBPlusEntry* entry, Data* data) {
	if (data->type == kDataBlock) {
		return data->block.size;
	}
	else if (data->type == kDataMemory) {
		MemoryData* mem_data = data->mem_buf.is_value ? &g_value : &g_key;
		return mem_data->size;
	}
	return 0;
}
forceinline void DataRelease(YuDbBPlusEntry* entry, Data* data) {
	BucketEntry* bucket_entry = BPlusEntryToBucketEntry(entry);
	if (data->type == kDataBlock) {
		BlockRelease(bucket_entry, data->block.element_id, data->block.size);
	}
}


/*
* B+树Element分配器
*/
int16_t YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR_CreateBySize(YuDbBPlusEntry* entry, int32_t size);
void YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR_Release(YuDbBPlusEntry* entry, int16_t element_id) {
	BucketEntry* bucket_entry = BPlusEntryToBucketEntry(entry);
	YuDbBPlusElement* element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(entry, element_id);
	//bucket_entry->info.alloc_size -= YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_GetNeedRate(entry, element);
	if (entry->type == kBPlusEntryIndex) {
		DataRelease(entry, &element->index.key);
		BlockRelease(bucket_entry, element_id, sizeof(YuDbBPlusIndexElement));
	}
	else {
		DataRelease(entry, &element->leaf.key);
		DataRelease(entry, &element->leaf.value);
		BlockRelease(bucket_entry, element_id, sizeof(YuDbBPlusLeafElement));
	}
	int16_t max_free;
	  assert(bucket_entry->info.alloc_size + (max_free = YuDbBPlusEntryFreeListGetFreeBlockSize(&bucket_entry->info.free_list, 0)) == bucket_entry->info.page_size - 2);
	YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(entry, element);
}
#define YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR

/*
* B+树Element访问器
*/
int32_t YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_GetNeedRate(YuDbBPlusEntry* dst_entry, YuDbBPlusEntry* src_entry, YuDbBPlusElement* element) {
	size_t size = 0;
	if (src_entry) {
		if (src_entry->type == kBPlusEntryIndex) {
			size += DataGetExpandSize(src_entry, &element->index.key);
		}
		else {
			size += DataGetExpandSize(src_entry, &element->leaf.key);
			size += DataGetExpandSize(src_entry, &element->leaf.value);
		}
	}
	else {
		if (dst_entry->type == kBPlusEntryIndex) {
			size += DataGetExpandSize(dst_entry, &element->index.key);
		}
		else {
			size += DataGetExpandSize(dst_entry, &element->leaf.key);
			size += DataGetExpandSize(dst_entry, &element->leaf.value);
		}
	}
	size += dst_entry->type == kBPlusEntryIndex ? sizeof(YuDbBPlusIndexElement) : sizeof(YuDbBPlusLeafElement);
	return size;
}
void YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_SetData(YuDbBPlusEntry* dst_entry, Data* dst, YuDbBPlusEntry* src_entry, const Data* src) {
	void* src_data_buf = NULL;
	void* dst_data_buf = NULL;
	size_t size = 1;
	DataType type = kDataMemory;
	if (src->type == kDataMemory) {
		MemoryData* data = src->mem_buf.is_value ? &g_value : &g_key;
		src_data_buf = data->mem_ptr;
		size = data->size;
		if (data->size <= sizeof(dst->inline_.data)) {
			dst_data_buf = dst->inline_.data;
			dst->inline_.size = size;
			dst->type = kDataInline;
		} else {
			size = data->size;
			goto lable_dst_block;
		}
	}
	else if (src->type == kDataInline) {
		src_data_buf = src->inline_.data;
		size = src->inline_.size;
		dst_data_buf = dst->inline_.data;
		dst->inline_.size = size;
		dst->type = kDataInline;
	}
	else if (src->type == kDataBlock) {
		src_data_buf = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(src_entry, src->block.element_id);;
		size = src->block.size;
	lable_dst_block:
		{
			int16_t element_id = YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR_CreateBySize(dst_entry, size);
			dst_data_buf = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(dst_entry, element_id);
			dst->block.size = size;
			dst->block.element_id = element_id;
			dst->type = kDataBlock;
		}
	}
	else {
		dst->type = src->type;
	}
	memcpy(dst_data_buf, src_data_buf, size);

	if (size > sizeof(dst->inline_.data)) {
		YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(dst_entry, dst_data_buf);
		if (src->type == kDataBlock) {
			YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(src_entry, src_data_buf);
			if (src_entry == dst_entry) {
				// 释放原先的块
				DataRelease(src_entry, dst);
			}
		}
	}
}
void YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_SetKey(YuDbBPlusEntry* dst_entry, YuDbBPlusElement* element, YuDbBPlusEntry* src_entry, YuDbKey* key) {
	if (dst_entry->type == kBPlusEntryIndex) {
		YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_SetData(dst_entry, &element->index.key, src_entry, key);
	} else {
		YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_SetData(dst_entry, &element->leaf.key, src_entry, key);
	}
}
void YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_SetValue(YuDbBPlusEntry* dst_entry, YuDbBPlusElement* element, YuDbBPlusEntry* src_entry, YuDbValue* value) {
	  assert(dst_entry->type == kBPlusEntryLeaf);
	YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_SetData(dst_entry, &element->leaf.value, src_entry, value);
}
#define YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR

/*
* B+树内嵌红黑树访问器
*/
YuDbKey* YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetKey(YuDbBPlusEntryRbTree* tree, YuDbBPlusEntryRbBsEntry* bs_entry) {
	if (((YuDbBPlusEntry*)tree)->type == kBPlusEntryLeaf) {
		return &((YuDbBPlusLeafElement*)bs_entry)->key;
	}
	else {
		return &((YuDbBPlusIndexElement*)bs_entry)->key;
	}
}
#define YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR

/*
* B+树比较器
*/
int32_t YUDB_BUCKET_BPLUS_COMPARER_Subrrac(YuDbBPlusEntryRbTree* tree, YuDbKey* key1, YuDbKey* key2) {
	YuDbBPlusEntry* entry = ObjectGetFromField(tree, YuDbBPlusEntry, rb_tree);
	void* key1_data, *key2_data;
	size_t key1_size, key2_size;
	key1_data = DataParser(entry, key1, &key1_size);
	key2_data = DataParser(entry, key2, &key2_size);
	ptrdiff_t res = 0;
	if (key1_size == key2_size) {
		res = MemoryCmpR(key1_data, key2_data, key1_size);
	}
	else {
		res = key1_size - key2_size;
	}
	if (key1_size > sizeof(key1->inline_.data)) {
		YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(entry, key1_data);
	}
	if (key2_size > sizeof(key2->inline_.data)) {
		YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(entry, key2_data);
	}
	return res;
}
bool YUDB_BUCKET_BPLUS_COMPARER_Equal(YuDbBPlusEntryRbTree* tree, YuDbKey* key1, YuDbKey* key2) {
	return YUDB_BUCKET_BPLUS_COMPARER_Subrrac(tree, key1, key2) == 0;
}
bool YUDB_BUCKET_BPLUS_COMPARER_Greater(YuDbBPlusEntryRbTree* tree, YuDbKey* key1, YuDbKey* key2) {
	return YUDB_BUCKET_BPLUS_COMPARER_Subrrac(tree, key1, key2) > 0;
}
bool YUDB_BUCKET_BPLUS_COMPARER_Less(YuDbBPlusEntryRbTree* tree, YuDbKey* key1, YuDbKey* key2) {
	return YUDB_BUCKET_BPLUS_COMPARER_Subrrac(tree, key1, key2) < 0;
}
#define YUDB_BUCKET_BPLUS_COMPARER YUDB_BUCKET_BPLUS_COMPARER

#define AAA
#ifndef AAA
CUTILS_CONTAINER_BPLUS_TREE_DEFINE(YuDb, CUTILS_CONTAINER_BPLUS_TREE_LEAF_LINK_MODE_NOT_LINK, 
	PageId, int16_t, YuDbKey, YuDbValue, CUTILS_OBJECT_ALLOCATOR_DEFALUT, YUDB_BUCKET_BPLUS_ENTRY_ALLOCATOR,
	YUDB_BUCKET_BPLUS_ENTRY_REFERENCER, YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR, YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR,
	YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER, YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR, 
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR, YUDB_BUCKET_BPLUS_COMPARER, 8)
#else
static const PageId YuDbBPlusLeafEntryReferencer_InvalidId = -1;
void YuDbBPlusCursorStackVectorResetCapacity(YuDbBPlusCursorStackVector* arr, size_t capacity) {
	YuDbBPlusElementPos* new_buf = ((YuDbBPlusElementPos*)MemoryAlloc(sizeof(YuDbBPlusElementPos) * (capacity)));
	if (arr->obj_arr) {
		memcpy((void*)(new_buf), (void*)(arr->obj_arr), (sizeof(YuDbBPlusElementPos) * arr->count));
		if (arr->is_vector_alloc) {
			(MemoryFree(arr->obj_arr));
		}
	}
	arr->is_vector_alloc = 1;
	arr->obj_arr = new_buf;
	arr->capacity = capacity;
}
void YuDbBPlusCursorStackVectorExpand(YuDbBPlusCursorStackVector* arr, size_t add_count) {
	size_t old_capacity = arr->capacity;
	size_t cur_capacity = old_capacity;
	size_t target_count = cur_capacity + add_count;
	if (cur_capacity == 0) {
		cur_capacity = 1;
	}
	while (cur_capacity < target_count) {
		cur_capacity *= 2;
	}
	YuDbBPlusCursorStackVectorResetCapacity(arr, cur_capacity);
	;
}
void YuDbBPlusCursorStackVectorInit(YuDbBPlusCursorStackVector* arr, size_t count, _Bool create) {
	arr->count = count;
	arr->obj_arr = ((void*)0);
	arr->is_vector_alloc = create;
	if (count != 0 && create) {
		YuDbBPlusCursorStackVectorResetCapacity(arr, count);
	}
	else {
		arr->capacity = count;
	}
}
void YuDbBPlusCursorStackVectorRelease(YuDbBPlusCursorStackVector* arr) {
	if (arr->obj_arr && arr->is_vector_alloc) {
		(MemoryFree(arr->obj_arr));
		arr->obj_arr = ((void*)0);
	}
	arr->capacity = 0;
	arr->count = 0;
}
ptrdiff_t YuDbBPlusCursorStackVectorPushTail(YuDbBPlusCursorStackVector* arr, const YuDbBPlusElementPos* obj) {
	if (arr->capacity <= arr->count) {
		YuDbBPlusCursorStackVectorExpand(arr, 1);
	}
	memcpy((void*)(&arr->obj_arr[arr->count++]), (void*)(obj), (sizeof(YuDbBPlusElementPos)));
	return arr->count - 1;
}
ptrdiff_t YuDbBPlusCursorStackVectorPushMultipleTail(YuDbBPlusCursorStackVector* arr, const YuDbBPlusElementPos* obj, size_t count) {
	if (arr->capacity <= arr->count + count) {
		YuDbBPlusCursorStackVectorExpand(arr, count);
	}
	memcpy((void*)(&arr->obj_arr[arr->count]), (void*)(obj), (sizeof(YuDbBPlusElementPos) * count));
	arr->count += count;
	return arr->count - count;
}
YuDbBPlusElementPos* YuDbBPlusCursorStackVectorPopTail(YuDbBPlusCursorStackVector* arr) {
	if (arr->count == 0) {
		return ((void*)0);
	}
	return &arr->obj_arr[--arr->count];
}
static const int16_t YuDbBPlusEntryRbReferencer_InvalidId = (-1);
__forceinline YuDbBPlusEntryRbEntry* YuDbBPlusEntryRbReferencer_Reference(YuDbBPlusEntryRbTree* tree, int16_t element_id) {
	if (element_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		return ((void*)0);
	}
	YuDbBPlusEntry* entry = ((YuDbBPlusEntry*)((uintptr_t)(tree)-((uintptr_t) & (((YuDbBPlusEntry*)0)->rb_tree))));
	return &(YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(entry, element_id)->rb_entry);
}
__forceinline void YuDbBPlusEntryRbReferencer_Dereference(YuDbBPlusEntryRbTree* tree, YuDbBPlusEntryRbEntry* rb_entry) {
	YuDbBPlusEntry* entry = ((YuDbBPlusEntry*)((uintptr_t)(tree)-((uintptr_t) & (((YuDbBPlusEntry*)0)->rb_tree))));
	YuDbBPlusElement* element = ((YuDbBPlusElement*)((uintptr_t)(rb_entry)-((uintptr_t) & (((YuDbBPlusElement*)0)->rb_entry))));
	YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(entry, element);
}
typedef struct {
	int16_t color : 1;
	int16_t parent : sizeof(int16_t) * 8 - 1;
}
YuDbBPlusEntryRbParentColor;
__forceinline int16_t YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(YuDbBPlusEntryRbTree* tree, YuDbBPlusEntryRbBsEntry* bs_entry) {
	return (((YuDbBPlusEntryRbParentColor*)&(((YuDbBPlusEntryRbEntry*)bs_entry)->parent_color))->parent);
}
__forceinline RbColor YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(YuDbBPlusEntryRbTree* tree, YuDbBPlusEntryRbBsEntry* bs_entry) {
	return (((YuDbBPlusEntryRbParentColor*)&(((YuDbBPlusEntryRbEntry*)bs_entry)->parent_color))->color == -1 ? 1 : 0);
}
__forceinline void YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(YuDbBPlusEntryRbTree* tree, YuDbBPlusEntryRbBsEntry* bs_entry, int16_t new_id) {
	((YuDbBPlusEntryRbParentColor*)&(((YuDbBPlusEntryRbEntry*)bs_entry)->parent_color))->parent = new_id;
}
__forceinline void YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(YuDbBPlusEntryRbTree* tree, YuDbBPlusEntryRbBsEntry* bs_entry, RbColor new_color) {
	return ((YuDbBPlusEntryRbParentColor*)&(((YuDbBPlusEntryRbEntry*)bs_entry)->parent_color))->color = new_color;
}
static void YuDbBPlusEntryRbBsTreeHitchEntry(YuDbBPlusEntryRbBsTree* tree, int16_t entry_id, int16_t new_entry_id) {
	YuDbBPlusEntryRbBsEntry* entry = YuDbBPlusEntryRbReferencer_Reference(tree, entry_id);
	YuDbBPlusEntryRbBsEntry* new_entry = YuDbBPlusEntryRbReferencer_Reference(tree, new_entry_id);
	if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, entry) != YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbBsEntry* entry_parent = YuDbBPlusEntryRbReferencer_Reference(tree, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, entry));
		if (entry_parent->left == entry_id) {
			entry_parent->left = new_entry_id;
		}
		else {
			entry_parent->right = new_entry_id;
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, &entry_parent);
	}
	if (new_entry_id != YuDbBPlusEntryRbReferencer_InvalidId) {
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, new_entry, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, entry));
	}
	if (tree->root == entry_id) {
		tree->root = new_entry_id;
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, entry);
	YuDbBPlusEntryRbReferencer_Dereference(tree, new_entry);
}
static int16_t YuDbBPlusEntryRbRotateLeft(YuDbBPlusEntryRbBsTree* tree, int16_t sub_root_id, YuDbBPlusEntryRbBsEntry* sub_root) {
	int16_t new_sub_root_id = sub_root->right;
	if (new_sub_root_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		return sub_root_id;
	}
	YuDbBPlusEntryRbBsEntry* new_sub_root = YuDbBPlusEntryRbReferencer_Reference(tree, new_sub_root_id);
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, new_sub_root, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, sub_root));
	if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, sub_root) != YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbBsEntry* sub_root_parent = YuDbBPlusEntryRbReferencer_Reference(tree, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, sub_root));
		if (sub_root_parent->left == sub_root_id) {
			sub_root_parent->left = new_sub_root_id;
		}
		else {
			sub_root_parent->right = new_sub_root_id;
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, sub_root_parent);
	}
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, sub_root, new_sub_root_id);
	sub_root->right = new_sub_root->left;
	if (sub_root->right != YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbBsEntry* sub_root_right = YuDbBPlusEntryRbReferencer_Reference(tree, sub_root->right);
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, sub_root_right, sub_root_id);
		YuDbBPlusEntryRbReferencer_Dereference(tree, sub_root_right);
	}
	new_sub_root->left = sub_root_id;
	YuDbBPlusEntryRbReferencer_Dereference(tree, new_sub_root);
	return new_sub_root_id;
}
static int16_t YuDbBPlusEntryRbRotateRight(YuDbBPlusEntryRbBsTree* tree, int16_t sub_root_id, YuDbBPlusEntryRbBsEntry* sub_root) {
	int16_t new_sub_root_id = sub_root->left;
	if (new_sub_root_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		return sub_root_id;
	}
	YuDbBPlusEntryRbBsEntry* new_sub_root = YuDbBPlusEntryRbReferencer_Reference(tree, new_sub_root_id);
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, new_sub_root, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, sub_root));
	if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, sub_root) != YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbBsEntry* sub_root_parent = YuDbBPlusEntryRbReferencer_Reference(tree, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, sub_root));
		if (sub_root_parent->left == sub_root_id) {
			sub_root_parent->left = new_sub_root_id;
		}
		else {
			sub_root_parent->right = new_sub_root_id;
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, sub_root_parent);
	}
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, sub_root, new_sub_root_id);
	sub_root->left = new_sub_root->right;
	if (sub_root->left != YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbBsEntry* sub_root_left = YuDbBPlusEntryRbReferencer_Reference(tree, sub_root->left);
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, sub_root_left, sub_root_id);
		YuDbBPlusEntryRbReferencer_Dereference(tree, sub_root_left);
	}
	new_sub_root->right = sub_root_id;
	YuDbBPlusEntryRbReferencer_Dereference(tree, new_sub_root);
	return new_sub_root_id;
}
static void YuDbBPlusEntryRbBsEntryInit(YuDbBPlusEntryRbBsTree* tree, YuDbBPlusEntryRbBsEntry* entry) {
	entry->left = YuDbBPlusEntryRbReferencer_InvalidId;
	entry->right = YuDbBPlusEntryRbReferencer_InvalidId;
	entry->parent = YuDbBPlusEntryRbReferencer_InvalidId;
}
void YuDbBPlusEntryRbBsTreeInit(YuDbBPlusEntryRbBsTree* tree) {
	tree->root = YuDbBPlusEntryRbReferencer_InvalidId;
}
int16_t YuDbBPlusEntryRbBsTreeFind(YuDbBPlusEntryRbBsTree* tree, YuDbKey* key) {
	int8_t status;
	int16_t id = YuDbBPlusEntryRbBsTreeIteratorLocate(tree, key, &status);
	return status == 0 ? id : YuDbBPlusEntryRbReferencer_InvalidId;
}
_Bool YuDbBPlusEntryRbBsTreeInsert(YuDbBPlusEntryRbBsTree* tree, int16_t entry_id) {
	YuDbBPlusEntryRbBsEntry* entry = YuDbBPlusEntryRbReferencer_Reference(tree, entry_id);
	if (tree->root == YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbBsEntryInit(tree, entry);
		tree->root = entry_id;
		return 1;
	}
	int16_t cur_id = tree->root;
	YuDbBPlusEntryRbBsEntry* cur = ((void*)0);
	_Bool success = 1;
	while (cur_id != YuDbBPlusEntryRbReferencer_InvalidId) {
		if (cur_id == entry_id) {
			success = 0;
			break;
		}
		YuDbBPlusEntryRbBsEntry* cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
		YuDbKey* cur_key = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetKey(tree, cur);
		YuDbKey* entry_key = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetKey(tree, entry);
		if (YUDB_BUCKET_BPLUS_COMPARER_Less(tree, cur_key, entry_key)) {
			if (cur->right == YuDbBPlusEntryRbReferencer_InvalidId) {
				cur->right = entry_id;
				break;
			}
			cur_id = cur->right;
		}
		else {
			if (cur_id == entry_id) break;
			if (cur->left == YuDbBPlusEntryRbReferencer_InvalidId) {
				cur->left = entry_id;
				break;
			}
			cur_id = cur->left;
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	}
	if (cur) YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	if (cur_id != entry_id) {
		YuDbBPlusEntryRbBsEntryInit(tree, entry);
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, entry, cur_id);
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, entry);
	return success;
}
int16_t YuDbBPlusEntryRbBsTreePut(YuDbBPlusEntryRbBsTree* tree, int16_t entry_id) {
	YuDbBPlusEntryRbBsEntry* entry = YuDbBPlusEntryRbReferencer_Reference(tree, entry_id);
	if (tree->root == YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbBsEntryInit(tree, entry);
		tree->root = entry_id;
		return YuDbBPlusEntryRbReferencer_InvalidId;
	}
	int16_t cur_id = tree->root;
	YuDbBPlusEntryRbBsEntry* cur = ((void*)0);
	int16_t old_id = YuDbBPlusEntryRbReferencer_InvalidId;
	while (cur_id != YuDbBPlusEntryRbReferencer_InvalidId) {
		cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
		YuDbKey* cur_key = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetKey(tree, cur);
		YuDbKey* entry_key = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetKey(tree, entry);
		if (YUDB_BUCKET_BPLUS_COMPARER_Less(tree, cur_key, entry_key)) {
			if (cur->right == YuDbBPlusEntryRbReferencer_InvalidId) {
				YuDbBPlusEntryRbBsEntryInit(tree, entry);
				cur->right = entry_id;
				break;
			}
			cur_id = cur->right;
		}
		else if (YUDB_BUCKET_BPLUS_COMPARER_Greater(tree, cur_key, entry_key)) {
			if (cur->left == YuDbBPlusEntryRbReferencer_InvalidId) {
				YuDbBPlusEntryRbBsEntryInit(tree, entry);
				cur->left = entry_id;
				break;
			}
			cur_id = cur->left;
		}
		else {
			old_id = cur_id;
			if (cur_id == entry_id) break;
			YuDbBPlusEntryRbBsEntryInit(tree, entry);
			int16_t parent_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
			if (parent_id != YuDbBPlusEntryRbReferencer_InvalidId) {
				YuDbBPlusEntryRbBsEntry* parent = YuDbBPlusEntryRbReferencer_Reference(tree, parent_id);
				if (parent->left == cur_id) {
					parent->left = entry_id;
				}
				else {
					parent->right = entry_id;
				}
			}
			else {
				tree->root = entry_id;
			}
			*entry = *cur;
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, entry, parent_id);
			if (cur->left != YuDbBPlusEntryRbReferencer_InvalidId) {
				YuDbBPlusEntryRbBsEntry* left = YuDbBPlusEntryRbReferencer_Reference(tree, cur->left);
				YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, left, entry_id);
				YuDbBPlusEntryRbReferencer_Dereference(tree, left);
			}
			if (cur->right != YuDbBPlusEntryRbReferencer_InvalidId) {
				YuDbBPlusEntryRbBsEntry* right = YuDbBPlusEntryRbReferencer_Reference(tree, cur->right);
				YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, right, entry_id);
				YuDbBPlusEntryRbReferencer_Dereference(tree, right);
			}
			break;
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	}
	if (cur) YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	if (old_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, entry, cur_id);
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, entry);
	return old_id;
}
int16_t YuDbBPlusEntryRbBsTreeDelete(YuDbBPlusEntryRbBsTree* tree, int16_t entry_id, _Bool* is_parent_left) {
	int16_t backtrack_id;
	YuDbBPlusEntryRbBsEntry* entry = YuDbBPlusEntryRbReferencer_Reference(tree, entry_id);
	if (entry->left != YuDbBPlusEntryRbReferencer_InvalidId && entry->right != YuDbBPlusEntryRbReferencer_InvalidId) {
		int16_t min_entry_id = entry->right;
		YuDbBPlusEntryRbBsEntry* min_entry = YuDbBPlusEntryRbReferencer_Reference(tree, min_entry_id);
		while (min_entry->left != YuDbBPlusEntryRbReferencer_InvalidId) {
			min_entry_id = min_entry->left;
			YuDbBPlusEntryRbReferencer_Dereference(tree, min_entry);
			min_entry = YuDbBPlusEntryRbReferencer_Reference(tree, min_entry_id);
		}
		YuDbBPlusEntryRbBsEntry* min_entry_parent = YuDbBPlusEntryRbReferencer_Reference(tree, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, min_entry));
		if (is_parent_left) {
			*is_parent_left = min_entry_parent->left == min_entry_id;
		}
		min_entry->left = entry->left;
		if (entry->left != YuDbBPlusEntryRbReferencer_InvalidId) {
			YuDbBPlusEntryRbBsEntry* entry_left = YuDbBPlusEntryRbReferencer_Reference(tree, entry->left);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, entry_left, min_entry_id);
			YuDbBPlusEntryRbReferencer_Dereference(tree, entry_left);
		}
		int16_t old_right_id = min_entry->right;
		if (entry->right != min_entry_id) {
			min_entry_parent->left = min_entry->right;
			if (min_entry->right != YuDbBPlusEntryRbReferencer_InvalidId) {
				YuDbBPlusEntryRbBsEntry* min_entry_right = YuDbBPlusEntryRbReferencer_Reference(tree, min_entry->right);
				YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, min_entry_right, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, min_entry));
				YuDbBPlusEntryRbReferencer_Dereference(tree, min_entry_right);
			}
			min_entry->right = entry->right;
			if (entry->right != YuDbBPlusEntryRbReferencer_InvalidId) {
				YuDbBPlusEntryRbBsEntry* entry_right = YuDbBPlusEntryRbReferencer_Reference(tree, entry->right);
				YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, entry_right, min_entry_id);
				YuDbBPlusEntryRbReferencer_Dereference(tree, entry_right);
			}
			backtrack_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, min_entry);
		}
		else {
			backtrack_id = min_entry_id;
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, min_entry_parent);
		YuDbBPlusEntryRbBsTreeHitchEntry(tree, entry_id, min_entry_id);
		entry_id = min_entry_id;
		entry->left = YuDbBPlusEntryRbReferencer_InvalidId;
		entry->right = old_right_id;
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, entry, backtrack_id);
	}
	else {
		if (is_parent_left) {
			int16_t parent_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, entry);
			if (parent_id != YuDbBPlusEntryRbReferencer_InvalidId) {
				YuDbBPlusEntryRbBsEntry* parent = YuDbBPlusEntryRbReferencer_Reference(tree, parent_id);
				*is_parent_left = parent->left == entry_id;
				YuDbBPlusEntryRbReferencer_Dereference(tree, parent);
			}
			else {
				*is_parent_left = 0;
			}
		}
		if (entry->right != YuDbBPlusEntryRbReferencer_InvalidId) {
			YuDbBPlusEntryRbBsTreeHitchEntry(tree, entry_id, entry->right);
		}
		else if (entry->left != YuDbBPlusEntryRbReferencer_InvalidId) {
			YuDbBPlusEntryRbBsTreeHitchEntry(tree, entry_id, entry->left);
		}
		else {
			YuDbBPlusEntryRbBsTreeHitchEntry(tree, entry_id, YuDbBPlusEntryRbReferencer_InvalidId);
		}
	}
	return entry_id;
}
size_t YuDbBPlusEntryRbBsTreeGetCount(YuDbBPlusEntryRbBsTree* tree) {
	size_t count = 0;
	int16_t cur_id = YuDbBPlusEntryRbBsTreeIteratorFirst(tree);
	while (cur_id != YuDbBPlusEntryRbReferencer_InvalidId) {
		count++;
		cur_id = YuDbBPlusEntryRbBsTreeIteratorNext(tree, cur_id);
	}
	return count;
}
int16_t YuDbBPlusEntryRbBsTreeIteratorLocate(YuDbBPlusEntryRbBsTree* tree, YuDbKey* key, int8_t* cmp_status) {
	int16_t cur_id = tree->root;
	int16_t perv_id = YuDbBPlusEntryRbReferencer_InvalidId;
	while (cur_id != YuDbBPlusEntryRbReferencer_InvalidId) {
		perv_id = cur_id;
		YuDbBPlusEntryRbBsEntry* cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
		YuDbKey* cur_key = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetKey(tree, cur);
		if (YUDB_BUCKET_BPLUS_COMPARER_Less(tree, cur_key, key)) {
			*cmp_status = 1;
			cur_id = cur->right;
		}
		else if (YUDB_BUCKET_BPLUS_COMPARER_Greater(tree, cur_key, key)) {
			*cmp_status = -1;
			cur_id = cur->left;
		}
		else {
			YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
			*cmp_status = 0;
			return cur_id;
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	}
	return perv_id;
}
int16_t YuDbBPlusEntryRbBsTreeIteratorFirst(YuDbBPlusEntryRbBsTree* tree) {
	int16_t cur_id = tree->root;
	if (cur_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		return YuDbBPlusEntryRbReferencer_InvalidId;
	}
	YuDbBPlusEntryRbBsEntry* cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
	while (cur->left != YuDbBPlusEntryRbReferencer_InvalidId) {
		cur_id = cur->left;
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
		cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	return cur_id;
}
int16_t YuDbBPlusEntryRbBsTreeIteratorLast(YuDbBPlusEntryRbBsTree* tree) {
	int16_t cur_id = tree->root;
	if (cur_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		return YuDbBPlusEntryRbReferencer_InvalidId;
	}
	YuDbBPlusEntryRbBsEntry* cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
	while (cur->right != YuDbBPlusEntryRbReferencer_InvalidId) {
		cur_id = cur->right;
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
		cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	return cur_id;
}
int16_t YuDbBPlusEntryRbBsTreeIteratorNext(YuDbBPlusEntryRbBsTree* tree, int16_t cur_id) {
	YuDbBPlusEntryRbBsEntry* cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
	if (cur->right != YuDbBPlusEntryRbReferencer_InvalidId) {
		cur_id = cur->right;
		cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
		while (cur->left != YuDbBPlusEntryRbReferencer_InvalidId) {
			cur_id = cur->left;
			YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
			cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
		return cur_id;
	}
	int16_t parent_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
	YuDbBPlusEntryRbBsEntry* parent = YuDbBPlusEntryRbReferencer_Reference(tree, parent_id);
	while (parent_id != YuDbBPlusEntryRbReferencer_InvalidId && cur_id == parent->right) {
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
		cur = parent;
		cur_id = parent_id;
		parent_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
		parent = YuDbBPlusEntryRbReferencer_Reference(tree, parent_id);
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	YuDbBPlusEntryRbReferencer_Dereference(tree, parent);
	return parent_id;
}
int16_t YuDbBPlusEntryRbBsTreeIteratorPrev(YuDbBPlusEntryRbBsTree* tree, int16_t cur_id) {
	YuDbBPlusEntryRbBsEntry* cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
	if (cur->left != YuDbBPlusEntryRbReferencer_InvalidId) {
		cur_id = cur->left;
		cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
		while (cur->right != YuDbBPlusEntryRbReferencer_InvalidId) {
			cur_id = cur->right;
			YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
			cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
		return cur_id;
	}
	int16_t parent_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
	YuDbBPlusEntryRbBsEntry* parent = YuDbBPlusEntryRbReferencer_Reference(tree, parent_id);
	while (parent_id != YuDbBPlusEntryRbReferencer_InvalidId && cur_id == parent->left) {
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
		cur = parent;
		cur_id = parent_id;
		parent_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
		parent = YuDbBPlusEntryRbReferencer_Reference(tree, parent_id);
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	YuDbBPlusEntryRbReferencer_Dereference(tree, parent);
	return parent_id;
}
static int16_t YuDbBPlusEntryGetSiblingEntry(YuDbBPlusEntryRbTree* tree, int16_t entry_id, YuDbBPlusEntryRbEntry* entry) {
	int16_t parent_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, entry);
	YuDbBPlusEntryRbEntry* parent = YuDbBPlusEntryRbReferencer_Reference(tree, parent_id);
	int16_t ret;
	if (parent->left == entry_id) {
		ret = parent->right;
	}
	else {
		ret = parent->left;
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, parent);
	return ret;
}
static void YuDbBPlusEntryRbTreeInsertFixup(YuDbBPlusEntryRbTree* tree, int16_t ins_entry_id) {
	YuDbBPlusEntryRbEntry* ins_entry = YuDbBPlusEntryRbReferencer_Reference(tree, ins_entry_id);
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, ins_entry, kRbBlack);
	int16_t cur_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, ins_entry);
	if (cur_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, ins_entry, kRbBlack);
		YuDbBPlusEntryRbReferencer_Dereference(tree, ins_entry);
		return;
	}
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, ins_entry, kRbRed);
	YuDbBPlusEntryRbReferencer_Dereference(tree, ins_entry);
	YuDbBPlusEntryRbEntry* cur = ((void*)0);
	while (cur_id != YuDbBPlusEntryRbReferencer_InvalidId) {
		cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
		if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, cur) == kRbBlack) {
			break;
		}
		if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur) == YuDbBPlusEntryRbReferencer_InvalidId) {
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, cur, kRbBlack);
			break;
		}
		int16_t sibling_id = YuDbBPlusEntryGetSiblingEntry(tree, cur_id, cur);
		YuDbBPlusEntryRbEntry* sibling = YuDbBPlusEntryRbReferencer_Reference(tree, sibling_id);
		if (sibling_id != YuDbBPlusEntryRbReferencer_InvalidId && YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, sibling) == kRbRed) {
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, cur, kRbBlack);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling, kRbBlack);
			ins_entry_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
			ins_entry = YuDbBPlusEntryRbReferencer_Reference(tree, ins_entry_id);
			if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, ins_entry) == YuDbBPlusEntryRbReferencer_InvalidId) {
				YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, ins_entry, kRbBlack);
				break;
			}
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, ins_entry, kRbRed);
			cur = ins_entry;
		}
		else {
			{
				if (!(sibling_id == YuDbBPlusEntryRbReferencer_InvalidId || YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, sibling) == kRbBlack)) {
					*(int*)0 = 0;
				}
			}
			;
			int16_t new_sub_root_id;
			int16_t old_sub_root_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
			YuDbBPlusEntryRbEntry* old_sub_root = YuDbBPlusEntryRbReferencer_Reference(tree, old_sub_root_id);
			if (old_sub_root->left == cur_id) {
				if (cur->right == ins_entry_id) {
					YuDbBPlusEntryRbRotateLeft(tree, cur_id, cur);
				}
				new_sub_root_id = YuDbBPlusEntryRbRotateRight(tree, old_sub_root_id, old_sub_root);
			}
			else {
				if (cur->left == ins_entry_id) {
					YuDbBPlusEntryRbRotateRight(tree, cur_id, cur);
				}
				new_sub_root_id = YuDbBPlusEntryRbRotateLeft(tree, old_sub_root_id, old_sub_root);
			}
			YuDbBPlusEntryRbEntry* new_sub_root = YuDbBPlusEntryRbReferencer_Reference(tree, new_sub_root_id);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, new_sub_root, kRbBlack);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, old_sub_root, kRbRed);
			YuDbBPlusEntryRbReferencer_Dereference(tree, new_sub_root);
			if (tree->root == old_sub_root_id) {
				tree->root = new_sub_root_id;
			}
			break;
		}
		cur_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
}
static void YuDbBPlusEntryRbTreeDeleteFixup(YuDbBPlusEntryRbTree* tree, int16_t del_entry_id, _Bool is_parent_left) {
	YuDbBPlusEntryRbEntry* del_entry = YuDbBPlusEntryRbReferencer_Reference(tree, del_entry_id);
	int16_t cur_id = YuDbBPlusEntryRbReferencer_InvalidId;
	RbColor del_color = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, del_entry);
	if (del_color == kRbRed) {
	}
	else if (del_entry->left != YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbEntry* del_entry_left = YuDbBPlusEntryRbReferencer_Reference(tree, del_entry->left);
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, del_entry_left, kRbBlack);
		YuDbBPlusEntryRbReferencer_Dereference(tree, del_entry_left);
	}
	else if (del_entry->right != YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbEntry* del_entry_right = YuDbBPlusEntryRbReferencer_Reference(tree, del_entry->right);
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, del_entry_right, kRbBlack);
		YuDbBPlusEntryRbReferencer_Dereference(tree, del_entry_right);
	}
	else {
		cur_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, del_entry);
	}
	int16_t new_sub_root_id;
	YuDbBPlusEntryRbEntry* cur = ((void*)0), * sibling = ((void*)0);
	cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
	while (cur_id != YuDbBPlusEntryRbReferencer_InvalidId) {
		int16_t sibling_id = is_parent_left ? cur->right : cur->left;
		sibling = YuDbBPlusEntryRbReferencer_Reference(tree, sibling_id);
		if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, sibling) == kRbRed) {
			int16_t old_sub_root_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, sibling);
			YuDbBPlusEntryRbEntry* old_sub_root = YuDbBPlusEntryRbReferencer_Reference(tree, old_sub_root_id);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, old_sub_root, kRbRed);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling, kRbBlack);
			if (old_sub_root->left == sibling_id) {
				new_sub_root_id = YuDbBPlusEntryRbRotateRight(tree, old_sub_root_id, old_sub_root);
				sibling_id = old_sub_root->left;
				YuDbBPlusEntryRbReferencer_Dereference(tree, sibling);
				sibling = YuDbBPlusEntryRbReferencer_Reference(tree, sibling_id);
			}
			else {
				new_sub_root_id = YuDbBPlusEntryRbRotateLeft(tree, old_sub_root_id, old_sub_root);
				sibling_id = old_sub_root->right;
				YuDbBPlusEntryRbReferencer_Dereference(tree, sibling);
				sibling = YuDbBPlusEntryRbReferencer_Reference(tree, sibling_id);
			}
			if (tree->root == old_sub_root_id) {
				tree->root = new_sub_root_id;
			}
			YuDbBPlusEntryRbReferencer_Dereference(tree, old_sub_root);
		}
		YuDbBPlusEntryRbEntry* sibling_right = YuDbBPlusEntryRbReferencer_Reference(tree, sibling->right);
		YuDbBPlusEntryRbEntry* sibling_left = YuDbBPlusEntryRbReferencer_Reference(tree, sibling->left);
		if (sibling->right != YuDbBPlusEntryRbReferencer_InvalidId && YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, sibling_right) == kRbRed || sibling->left != YuDbBPlusEntryRbReferencer_InvalidId && YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, sibling_left) == kRbRed) {
			RbColor parent_color = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, cur);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, cur, kRbBlack);
			int16_t old_sub_root_id = cur_id;
			if (cur->left == sibling_id) {
				if (sibling->left == YuDbBPlusEntryRbReferencer_InvalidId || YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, sibling_left) == kRbBlack) {
					YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling_right, kRbBlack);
					sibling_id = YuDbBPlusEntryRbRotateLeft(tree, sibling_id, sibling);
				}
				else {
					YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling_left, kRbBlack);
				}
				new_sub_root_id = YuDbBPlusEntryRbRotateRight(tree, cur_id, cur);
			}
			else {
				if (sibling->right == YuDbBPlusEntryRbReferencer_InvalidId || YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, sibling_right) == kRbBlack) {
					YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling_left, kRbBlack);
					sibling_id = YuDbBPlusEntryRbRotateRight(tree, sibling_id, sibling);
				}
				else {
					YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling_right, kRbBlack);
				}
				new_sub_root_id = YuDbBPlusEntryRbRotateLeft(tree, cur_id, cur);
			}
			YuDbBPlusEntryRbReferencer_Dereference(tree, sibling);
			sibling = YuDbBPlusEntryRbReferencer_Reference(tree, sibling_id);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling, parent_color);
			if (tree->root == old_sub_root_id) {
				tree->root = new_sub_root_id;
			}
			YuDbBPlusEntryRbReferencer_Dereference(tree, sibling_right);
			YuDbBPlusEntryRbReferencer_Dereference(tree, sibling_left);
			break;
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, sibling_right);
		YuDbBPlusEntryRbReferencer_Dereference(tree, sibling_left);
		if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, cur) == kRbRed) {
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling, kRbRed);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, cur, kRbBlack);
			break;
		}
		else {
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling, kRbRed);
		}
		int16_t child_id = cur_id;
		cur_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
		if (cur_id != YuDbBPlusEntryRbReferencer_InvalidId) {
			YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
			cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
			is_parent_left = cur->left == child_id;
		}
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, sibling);
	YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	YuDbBPlusEntryRbEntry* root = YuDbBPlusEntryRbReferencer_Reference(tree, tree->root);
	if (root && YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, root) == kRbRed) {
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, root, kRbBlack);
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, root);
}
void YuDbBPlusEntryRbTreeInit(YuDbBPlusEntryRbTree* tree) {
	YuDbBPlusEntryRbBsTreeInit(&tree->bs_tree);
}
int16_t YuDbBPlusEntryRbTreeFind(YuDbBPlusEntryRbTree* tree, YuDbKey* key) {
	return YuDbBPlusEntryRbBsTreeFind(&tree->bs_tree, key);
}
_Bool YuDbBPlusEntryRbTreeInsert(YuDbBPlusEntryRbTree* tree, int16_t insert_entry_id) {
	if (!YuDbBPlusEntryRbBsTreeInsert(&tree->bs_tree, insert_entry_id)) return 0;
	YuDbBPlusEntryRbTreeInsertFixup(tree, insert_entry_id);
	return 1;
}
int16_t YuDbBPlusEntryRbTreePut(YuDbBPlusEntryRbTree* tree, int16_t put_entry_id) {
	int16_t old_id = YuDbBPlusEntryRbBsTreePut(&tree->bs_tree, put_entry_id);
	if (old_id == YuDbBPlusEntryRbReferencer_InvalidId) YuDbBPlusEntryRbTreeInsertFixup(tree, put_entry_id);
	return old_id;
}
_Bool YuDbBPlusEntryRbTreeDelete(YuDbBPlusEntryRbTree* tree, int16_t del_entry_id) {
	_Bool is_parent_left;
	int16_t del_min_entry_id = YuDbBPlusEntryRbBsTreeDelete(&tree->bs_tree, del_entry_id, &is_parent_left);
	if (del_min_entry_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		return 0;
	}
	if (del_min_entry_id != del_entry_id) {
		YuDbBPlusEntryRbEntry* del_entry = YuDbBPlusEntryRbReferencer_Reference(tree, del_entry_id);
		YuDbBPlusEntryRbEntry* del_min_entry = YuDbBPlusEntryRbReferencer_Reference(tree, del_min_entry_id);
		;
		RbColor old_color = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, del_min_entry);
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, del_min_entry, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, del_entry));
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, del_entry, old_color);
	}
	YuDbBPlusEntryRbTreeDeleteFixup(tree, del_entry_id, is_parent_left);
	return 1;
}
int16_t YuDbBPlusEntryRbTreeIteratorLocate(YuDbBPlusEntryRbTree* tree, YuDbKey* key, int8_t* cmp_status) {
	return YuDbBPlusEntryRbBsTreeIteratorLocate((YuDbBPlusEntryRbBsTree*)tree, key, cmp_status);
}
int16_t YuDbBPlusEntryRbTreeIteratorFirst(YuDbBPlusEntryRbTree* tree) {
	return YuDbBPlusEntryRbBsTreeIteratorFirst((YuDbBPlusEntryRbBsTree*)tree);
}
int16_t YuDbBPlusEntryRbTreeIteratorLast(YuDbBPlusEntryRbTree* tree) {
	return YuDbBPlusEntryRbBsTreeIteratorLast((YuDbBPlusEntryRbBsTree*)tree);
}
int16_t YuDbBPlusEntryRbTreeIteratorNext(YuDbBPlusEntryRbTree* tree, int16_t cur_id) {
	return YuDbBPlusEntryRbBsTreeIteratorNext((YuDbBPlusEntryRbBsTree*)tree, cur_id);
}
int16_t YuDbBPlusEntryRbTreeIteratorPrev(YuDbBPlusEntryRbTree* tree, int16_t cur_id) {
	return YuDbBPlusEntryRbBsTreeIteratorPrev((YuDbBPlusEntryRbBsTree*)tree, cur_id);
}
static void YuDbBPlusElementSet(YuDbBPlusEntry* dst_entry, int16_t element_id, YuDbBPlusEntry* src_entry, YuDbBPlusElement* element, PageId element_child_id) {
	{
		if (!(element_id >= 0)) {
			*(int*)0 = 0;
		}
	}
	;
	YuDbBPlusElement* dst_element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(dst_entry, element_id);
	if (dst_entry->type == kBPlusEntryLeaf) {
		YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_SetKey(dst_entry, dst_element, src_entry, &element->leaf.key);
		YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_SetValue(dst_entry, dst_element, src_entry, &element->leaf.value);
	}
	else {
		if (src_entry->type == kBPlusEntryLeaf) {
			YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_SetKey(dst_entry, dst_element, src_entry, &element->leaf.key); {
				if (!(element_child_id != -1)) {
					*(int*)0 = 0;
				}
			}
			;
			dst_element->index.child_id = element_child_id;
		}
		else {
			YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_SetKey(dst_entry, dst_element, src_entry, &element->index.key);
			if (element_child_id != -1) {
				dst_element->index.child_id = element_child_id;
			}
			else {
				dst_element->index.child_id = element->index.child_id;
			}
		}
	}
	YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(dst_entry, dst_element);
}
static PageId YuDbBPlusElementGetChildId(const YuDbBPlusEntry* index, int16_t element_id) {
	if (element_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		return index->index.tail_child_id;
	}
	YuDbBPlusElement* element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(index, element_id);
	PageId child_id = element->index.child_id;
	YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(index, element);
	return child_id;
}
static void YuDbBPlusElementSetChildId(YuDbBPlusEntry* index, int16_t element_id, PageId entry_id) {
	if (element_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		index->index.tail_child_id = entry_id;
		return;
	}
	YuDbBPlusElement* element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(index, element_id);
	element->index.child_id = entry_id;
	YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(index, element);
}
static int16_t YuDbBPlusElementCreate(YuDbBPlusEntry* entry) {
	int16_t element_id = YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR_CreateBySize(entry, entry->type == kBPlusEntryLeaf ? sizeof(YuDbBPlusLeafElement) : sizeof(YuDbBPlusIndexElement)); {
		if (!(element_id >= 0)) {
			*(int*)0 = 0;
		}
	}
	;
	return element_id;
}
static YuDbBPlusElement* YuDbBPlusElementRelease(YuDbBPlusEntry* entry, int16_t element_id) {
	{
		if (!(element_id >= 0)) {
			*(int*)0 = 0;
		}
	}
	;
	YuDbBPlusElement* element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(entry, element_id);
	YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(entry, element);
	YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR_Release(entry, element_id);
	return element;
}
YuDbBPlusElementPos* YuDbBPlusCursorCur(YuDbBPlusTree* tree, YuDbBPlusCursor* cursor) {
	if (cursor->level < 0) {
		return ((void*)0);
	}
	return &cursor->stack.obj_arr[cursor->level];
}
YuDbBPlusElementPos* YuDbBPlusCursorUp(YuDbBPlusTree* tree, YuDbBPlusCursor* cursor) {
	if (cursor->level <= 0) {
		return ((void*)0);
	}
	return &cursor->stack.obj_arr[--cursor->level];
}
YuDbBPlusElementPos* YuDbBPlusCursorDown(YuDbBPlusTree* tree, YuDbBPlusCursor* cursor) {
	if (cursor->level + 1 >= cursor->stack.count) {
		return ((void*)0);
	}
	return &cursor->stack.obj_arr[++cursor->level];
}
BPlusCursorStatus YuDbBPlusCursorFirst(YuDbBPlusTree* tree, YuDbBPlusCursor* cursor, YuDbKey* key) {
	YuDbBPlusCursorStackVectorInit(&cursor->stack, 8, 0);
	cursor->stack.obj_arr = cursor->default_stack;
	cursor->stack.count = 0;
	cursor->level = -1;
	return YuDbBPlusCursorNext(tree, cursor, key);
}
void YuDbBPlusCursorRelease(YuDbBPlusTree* tree, YuDbBPlusCursor* cursor) {
	YuDbBPlusCursorStackVectorRelease(&cursor->stack);
}
BPlusCursorStatus YuDbBPlusCursorNext(YuDbBPlusTree* tree, YuDbBPlusCursor* cursor, YuDbKey* key) {
	YuDbBPlusElementPos cur;
	YuDbBPlusElementPos* parent = YuDbBPlusCursorCur(tree, cursor);
	if (parent) {
		YuDbBPlusEntry* parent_entry = YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(tree, parent->entry_id);
		if (parent_entry->type == kBPlusEntryLeaf) {
			return kBPlusCursorEnd;
		}
		cur.entry_id = YuDbBPlusElementGetChildId(parent_entry, parent->element_id);
		YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(tree, parent_entry);
	}
	else {
		cur.entry_id = tree->root_id;
	}
	YuDbBPlusEntry* cur_entry = YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(tree, cur.entry_id);
	int8_t cmp_status = -1;
	if (cur_entry->element_count > 0) {
		cur.element_id = YuDbBPlusEntryRbTreeIteratorLocate(&cur_entry->rb_tree, key, &cmp_status);
		if (cmp_status == -1) {
		}
		else {
			if (cur_entry->type == kBPlusEntryIndex || cmp_status == 1) {
				cur.element_id = YuDbBPlusEntryRbTreeIteratorNext(&cur_entry->rb_tree, cur.element_id);
			}
		}
	}
	else {
		cur.element_id = YuDbBPlusEntryRbReferencer_InvalidId;
	}
	YuDbBPlusCursorStackVectorPushTail(&cursor->stack, &cur);
	BPlusCursorStatus status = kBPlusCursorNext;
	if (cur_entry->type == kBPlusEntryLeaf) {
		if (cmp_status != 0) {
			status = kBPlusCursorNe;
		}
		else {
			status = kBPlusCursorEq;
		}
		cursor->leaf_status = status;
	}
	++cursor->level;
	YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(tree, cur_entry);
	return status;
}
static int16_t YuDbBPlusEntryInsertElement(YuDbBPlusEntry* dst_entry, YuDbBPlusEntry* src_entry, YuDbBPlusElement* insert_element, PageId element_child_id) {
	int16_t element_id = YuDbBPlusElementCreate(dst_entry); {
		if (!(element_id != YuDbBPlusEntryRbReferencer_InvalidId)) {
			*(int*)0 = 0;
		}
	}
	;
	YuDbBPlusElementSet(dst_entry, element_id, src_entry, insert_element, element_child_id);
	int16_t old_element_id = YuDbBPlusEntryRbTreePut(&dst_entry->rb_tree, element_id);
	if (old_element_id != YuDbBPlusEntryRbReferencer_InvalidId && old_element_id != element_id) YuDbBPlusElementRelease(dst_entry, old_element_id);
	dst_entry->element_count++;
	return element_id;
}
static void YuDbBPlusEntryDeleteElement(YuDbBPlusEntry* entry, int16_t element_id) {
	{
		if (!(element_id != YuDbBPlusEntryRbReferencer_InvalidId)) {
			*(int*)0 = 0;
		}
	}
	;
	YuDbBPlusEntryRbTreeDelete(&entry->rb_tree, element_id);
	entry->element_count--;
	YuDbBPlusElementRelease(entry, element_id);
}
PageId YuDbBPlusEntryCreate(YuDbBPlusTree* tree, BPlusEntryType type) {
	size_t size;
	PageId entry_id;
	if (type == kBPlusEntryIndex) {
		entry_id = YUDB_BUCKET_BPLUS_ENTRY_ALLOCATOR_CreateBySize(tree, sizeof(YuDbBPlusEntry) - sizeof(YuDbBPlusLeafEntry) + sizeof(YuDbBPlusIndexEntry));
	}
	else {
		entry_id = YUDB_BUCKET_BPLUS_ENTRY_ALLOCATOR_CreateBySize(tree, sizeof(YuDbBPlusEntry) - sizeof(YuDbBPlusIndexEntry) + sizeof(YuDbBPlusLeafEntry));
	}
	YuDbBPlusEntry* entry = YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(tree, entry_id);
	entry->type = type;
	entry->element_count = 0;
	YuDbBPlusEntryRbTreeInit(&entry->rb_tree);
	YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(tree, entry);
	return entry_id;
}
void YuDbBPlusEntryRelease(YuDbBPlusTree* tree, PageId entry_id) {
	YUDB_BUCKET_BPLUS_ENTRY_ALLOCATOR_Release(tree, entry_id);
}
static int16_t YuDbBuildRbTree(YuDbBPlusEntry* src_entry, int16_t* src_entry_rb_iter, YuDbBPlusEntry* dst_entry, int32_t left, int32_t right, int16_t parent_id, int32_t max_level, int32_t level) {
	if (left > right) return (-1);
	int16_t mid = (left + right + 1) / 2;
	int16_t new_element_id = YuDbBPlusElementCreate(dst_entry);
	YuDbBPlusElement* new_element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(dst_entry, new_element_id);
	YuDbBPlusEntryRbEntry* rb_entry = &new_element->rb_entry;
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(&dst_entry->rb_tree, rb_entry, parent_id);
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(&dst_entry->rb_tree, rb_entry, level == max_level && max_level > 1 ? kRbRed : kRbBlack);
	rb_entry->left = YuDbBuildRbTree(src_entry, src_entry_rb_iter, dst_entry, left, mid - 1, new_element_id, max_level, level + 1);
	  assert(*src_entry_rb_iter != -1);
	YuDbBPlusElement* src_element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(src_entry, *src_entry_rb_iter);
	YuDbBPlusElementSet(dst_entry, new_element_id, src_entry, src_element, -1);
	YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(src_entry, src_element);
	*src_entry_rb_iter = YuDbBPlusEntryRbTreeIteratorNext(&src_entry->rb_tree, *src_entry_rb_iter);
	rb_entry->right = YuDbBuildRbTree(src_entry, src_entry_rb_iter, dst_entry, mid + 1, right, new_element_id, max_level, level + 1);
	YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(dst_entry, new_element);
	return new_element_id;
}
static int16_t YuDbBPlusEntrySplit(YuDbBPlusTree* tree, YuDbBPlusEntry* left, PageId left_id, YuDbBPlusEntry* parent, int16_t parent_element_id, YuDbBPlusEntry** src_entry, YuDbBPlusElement* insert_element, int16_t insert_id, PageId insert_element_child_id, _Bool dereference_src_entry, PageId* out_right_id) {
	PageId right_id = YuDbBPlusEntryCreate(tree, left->type);
	YuDbBPlusEntry* right = YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(tree, right_id);
	int16_t up_element_id;
	if (left->type == kBPlusEntryLeaf) {
	}
	int16_t left_element_id = YuDbBPlusEntryRbTreeIteratorLast(&left->rb_tree);
	_Bool insert_right = 0;
	int32_t fill_rate = (YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR_GetFillRate(tree, left) + YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_GetNeedRate(right, *src_entry, insert_element)) / 2;
	int16_t right_count = 1, left_count = 0;
	int32_t left_fill_rate = YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR_GetFillRate(tree, left);
	while (true) {
		if (!insert_right && left_element_id == insert_id) {
			insert_right = 1;
		}
		if (left_fill_rate <= fill_rate || left->element_count <= 2) {
			break;
		}
		left_fill_rate -= YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_GetNeedRate(left, right, insert_element);
		left_element_id = YuDbBPlusEntryRbTreeIteratorPrev(&left->rb_tree, left_element_id);
		++right_count;
		
	} {
		if (!(left_element_id != (-1))) {
			*(int*)0 = 0;
		}
	}
	; {
		if (!(right_count > 0)) {
			*(int*)0 = 0;
		}
	}
	;
	int16_t temp_left_element_id = left_element_id;
	int32_t logn = right_count == 1 ? -1 : 0;
	for (int32_t i = right_count; i > 0; i /= 2) ++logn;
	right->rb_tree.root = YuDbBuildRbTree(left, &temp_left_element_id, right, 0, right_count - 1, (-1), logn, 0);
	YuDbBPlusEntry* temp_entry = YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR_GetTempCopyEntry(tree, left);
	YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR_Clean(tree, left);

	do {
		left_element_id = YuDbBPlusEntryRbTreeIteratorPrev(&temp_entry->rb_tree, left_element_id);
		++left_count;
	} while (left_element_id != (-1));
	--left_count;
	left_element_id = YuDbBPlusEntryRbTreeIteratorFirst(&temp_entry->rb_tree);
	logn = left_count == 1 ? -1 : 0;
	for (int32_t i = left_count; i > 0; i /= 2) ++logn;
	left->rb_tree.root = YuDbBuildRbTree(temp_entry, &left_element_id, left, 0, left_count - 1, (-1), logn, 0);

	if (insert_right) {
		YuDbBPlusEntryInsertElement(right, *src_entry, insert_element, insert_element_child_id);
		right_count++;
	}
	else {
		YuDbBPlusEntryInsertElement(left, *src_entry, insert_element, insert_element_child_id);
		left_count++;
	}
	left->element_count = left_count;
	right->element_count = right_count;
	if (dereference_src_entry && *src_entry) YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(tree, *src_entry);
	YuDbBPlusElement* up_element;
	if (left->type == kBPlusEntryLeaf) {
		*src_entry = right;
		YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(tree, right_id);
		up_element_id = YuDbBPlusEntryRbTreeIteratorFirst(&right->rb_tree);
		up_element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(*src_entry, up_element_id);
	}
	else {
		right->index.tail_child_id = left->index.tail_child_id;
		up_element_id = YuDbBPlusEntryRbTreeIteratorLast(&left->rb_tree);
		*src_entry = left;
		YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(tree, left_id);
		up_element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(*src_entry, up_element_id);
		left->index.tail_child_id = up_element->index.child_id;
	}
	

	YuDbBPlusElementSetChildId(parent, parent_element_id, right_id);
	*out_right_id = right_id;
	YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(tree, right); {
		if (!(left->element_count >= 1)) {
			*(int*)0 = 0;
		}
	}
	; {
		if (!(right->element_count >= 1)) {
			*(int*)0 = 0;
		}
	}
	;
	return up_element_id;
}
static void YuDbBPlusEntryMerge(YuDbBPlusTree* tree, YuDbBPlusEntry* left, PageId left_id, YuDbBPlusEntry* right, PageId right_id, YuDbBPlusEntry* parent, int16_t parent_index) {
	int16_t right_element_id = YuDbBPlusEntryRbTreeIteratorLast(&right->rb_tree);
	for (int32_t i = 0; i < right->element_count; i++) {
		{
			if (!(right_element_id != YuDbBPlusEntryRbReferencer_InvalidId)) {
				*(int*)0 = 0;
			}
		}
		;
		YuDbBPlusElement* right_element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(right, right_element_id);
		YuDbBPlusEntryInsertElement(left, ((void*)0), right_element, -1);
		YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(right, right_element);
		right_element_id = YuDbBPlusEntryRbTreeIteratorPrev(&right->rb_tree, right_element_id);
	}
	if (left->type == kBPlusEntryLeaf) {
	}
	else {
		YuDbBPlusElement* parent_element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(parent, parent_index);
		int16_t left_element_id = YuDbBPlusEntryInsertElement(left, ((void*)0), parent_element, -1);
		YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(parent, parent_element);
		YuDbBPlusElementSetChildId(left, left_element_id, left->index.tail_child_id);
		YuDbBPlusElementSetChildId(left, -1, right->index.tail_child_id);
	}
	YuDbBPlusElementSetChildId(parent, YuDbBPlusEntryRbTreeIteratorNext(&parent->rb_tree, parent_index), left_id);
	YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(tree, right);
	YuDbBPlusEntryRelease(tree, right_id);
}
static _Bool YuDbBPlusTreeInsertElement(YuDbBPlusTree* tree, YuDbBPlusCursor* cursor, YuDbBPlusEntry* src_entry, YuDbBPlusElement* insert_element, int16_t insert_element_id, PageId insert_element_child_id) {
	YuDbBPlusElementPos* cur_pos = YuDbBPlusCursorCur(tree, cursor);
	YuDbBPlusElementPos* parent_pos = YuDbBPlusCursorUp(tree, cursor);
	PageId right_id;
	YuDbBPlusEntry* cur = YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(tree, cur_pos->entry_id);
	_Bool success = 1, insert_up = 0;
	int16_t up_element_id = (-1);
	do {
		int32_t free_rate = YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR_GetFreeRate(tree, cur);
		int32_t need_rate = YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_GetNeedRate(cur, src_entry, insert_element);
		if (cursor->leaf_status == kBPlusCursorEq) {
			break;
			// ---
			YuDbBPlusElement* raw = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(cur, cur_pos->element_id);
			int32_t raw_rate = YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_GetNeedRate(cur, cur, raw);
			YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(cur, raw);
			if (free_rate /* + need_rate */ >= raw_rate) {
				/* 实际上可以加上need_rate，为了设计符合直觉要求有足够的拷贝空间 */
				YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_SetValue(cur, insert_element, cur, &insert_element->leaf.value);
				break;
			}
			else {
				/* 空间不足将会触发分裂，先将其删除 */
				cursor->leaf_status = kBPlusCursorNe;
				int16_t temp = YuDbBPlusEntryRbTreeIteratorNext(&cur->rb_tree, cur_pos->element_id);
				if (temp != -1) {
					temp = YuDbBPlusEntryRbTreeIteratorNext(&cur->rb_tree, temp);
				}
				YuDbBPlusEntryDeleteElement(cur, cur_pos->element_id);
				cur_pos->element_id = temp;
			}
			// ---
		}
		else 
			if (free_rate >= need_rate) {
			YuDbBPlusEntryInsertElement(cur, src_entry, insert_element, insert_element_child_id);
			break;
		}
		if (cur_pos->element_id == YuDbBPlusEntryRbReferencer_InvalidId) {
			cur_pos->element_id = YuDbBPlusEntryRbTreeIteratorLast(&cur->rb_tree);
		}
		else {
			cur_pos->element_id = YuDbBPlusEntryRbTreeIteratorPrev(&cur->rb_tree, cur_pos->element_id);
		}
		if (!parent_pos) {
			PageId parent_id = YuDbBPlusEntryCreate(tree, kBPlusEntryIndex);
			YuDbBPlusEntry* parent = YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(tree, parent_id);
			YuDbBPlusEntry* new_src_entry = src_entry;
			up_element_id = YuDbBPlusEntrySplit(tree, cur, cur_pos->entry_id, parent, -1, &new_src_entry, insert_element, cur_pos->element_id, insert_element_child_id, 0, &right_id);
			YuDbBPlusElement* up_element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(new_src_entry, up_element_id);
			YuDbBPlusEntryInsertElement(parent, new_src_entry, up_element, cur_pos->entry_id);
			YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(new_src_entry, up_element);
			YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(tree, new_src_entry);
			tree->root_id = parent_id;
			YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(tree, parent);
			break;
		}
		YuDbBPlusEntry* parent = YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(tree, parent_pos->entry_id);
		up_element_id = YuDbBPlusEntrySplit(tree, cur, cur_pos->entry_id, parent, parent_pos->element_id, &src_entry, insert_element, cur_pos->element_id, insert_element_child_id, 1, &right_id);
		YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(tree, parent);
		insert_up = 1;
	} while (0);
	YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(tree, cur);
	if (insert_up) {
		YuDbBPlusElement* up_element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(src_entry, up_element_id);
		return YuDbBPlusTreeInsertElement(tree, cursor, src_entry, up_element, up_element_id, cur_pos->entry_id);
	}
	if (src_entry) {
		YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(src_entry, insert_element);
		if (src_entry->type == kBPlusEntryIndex) YuDbBPlusEntryDeleteElement(src_entry, insert_element_id);
		YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(tree, src_entry);
	}
	return success;
}
static _Bool YuDbBPlusTreeDeleteElement(YuDbBPlusTree* tree, YuDbBPlusCursor* cursor) {
	YuDbBPlusElementPos* cur_pos = YuDbBPlusCursorCur(tree, cursor);
	YuDbBPlusElementPos* parent_pos = YuDbBPlusCursorUp(tree, cursor);
	YuDbBPlusEntry* entry = YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(tree, cur_pos->entry_id);
	PageId sibling_entry_id = 0;
	YuDbBPlusEntry* sibling = ((void*)0);
	YuDbBPlusEntry* parent = ((void*)0);
	_Bool success = 1, delete_up = 0;
	YuDbBPlusEntryDeleteElement(entry, cur_pos->element_id);
	do {
		if (YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR_GetFillRate(tree, entry) >= YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR_GetMergeThresholdRate(tree, entry)) {
			break;
		}
		if (!parent_pos) {
			if (entry->type == kBPlusEntryIndex && entry->element_count == 0) {
				PageId childId = entry->index.tail_child_id;
				tree->root_id = childId;
				YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(tree, entry);
				YuDbBPlusEntryRelease(tree, cur_pos->entry_id);
				return 1;
			}
			else {
				break;
			}
		}
		parent = YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(tree, parent_pos->entry_id);
		_Bool left_sibling = 1;
		int16_t common_parent_element_id = parent_pos->element_id;
		int16_t sibling_element_id;
		if (common_parent_element_id == YuDbBPlusEntryRbReferencer_InvalidId) {
			sibling_element_id = YuDbBPlusEntryRbTreeIteratorLast(&parent->rb_tree);
		}
		else {
			sibling_element_id = YuDbBPlusEntryRbTreeIteratorPrev(&parent->rb_tree, common_parent_element_id);
			if (sibling_element_id == YuDbBPlusEntryRbReferencer_InvalidId) {
				left_sibling = 0;
				sibling_element_id = YuDbBPlusEntryRbTreeIteratorNext(&parent->rb_tree, common_parent_element_id);
				if (sibling_element_id == YuDbBPlusEntryRbReferencer_InvalidId) {
					sibling_entry_id = parent->index.tail_child_id;
				}
			}
		}
		if (sibling_element_id != YuDbBPlusEntryRbReferencer_InvalidId) {
			YuDbBPlusElement* sibling_element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(parent, sibling_element_id);
			sibling_entry_id = sibling_element->index.child_id;
			YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(parent, sibling_element);
		}
		if (left_sibling) {
			common_parent_element_id = sibling_element_id;
			parent_pos->element_id = sibling_element_id;
		} {
			if (!(common_parent_element_id != YuDbBPlusEntryRbReferencer_InvalidId)) {
				*(int*)0 = 0;
			}
		}
		; {
			if (!(sibling_entry_id != -1)) {
				*(int*)0 = 0;
			}
		}
		;
		sibling = YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(tree, sibling_entry_id);
		if (YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR_GetFillRate(tree, sibling) > YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR_GetMergeThresholdRate(tree, sibling)) {
			{
				if (!(sibling->element_count >= 2)) {
					*(int*)0 = 0;
				}
			}
			;
			if (entry->type == kBPlusEntryLeaf) {
				if (left_sibling) {
					int16_t last = YuDbBPlusEntryRbTreeIteratorLast(&sibling->rb_tree); {
						if (!(last != YuDbBPlusEntryRbReferencer_InvalidId)) {
							*(int*)0 = 0;
						}
					}
					;
					YuDbBPlusElement* element = YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(sibling, last);
					YuDbBPlusEntryInsertElement(entry, sibling, element, -1);
					YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(sibling, element);
					YuDbBPlusEntryDeleteElement(sibling, last);
					YuDbBPlusElement* common_parent_element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(parent, common_parent_element_id);
					common_parent_element->index.key = element->leaf.key;
					YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(parent, common_parent_element);
				}
				else {
					int16_t first = YuDbBPlusEntryRbTreeIteratorFirst(&sibling->rb_tree);
					int16_t new_first = YuDbBPlusEntryRbTreeIteratorNext(&sibling->rb_tree, first); {
						if (!(first != YuDbBPlusEntryRbReferencer_InvalidId)) {
							*(int*)0 = 0;
						}
					}
					; {
						if (!(new_first != YuDbBPlusEntryRbReferencer_InvalidId)) {
							*(int*)0 = 0;
						}
					}
					;
					YuDbBPlusElement* element = YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(sibling, first);
					YuDbBPlusEntryInsertElement(entry, sibling, element, -1);
					YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(sibling, element);
					YuDbBPlusEntryDeleteElement(sibling, first);
					YuDbBPlusElement* common_parent_element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(parent, common_parent_element_id);
					YuDbBPlusElement* sibling_element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(sibling, new_first);
					common_parent_element->index.key = sibling_element->leaf.key;
					YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(parent, common_parent_element);
					YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(sibling, sibling_element);
				}
			}
			else {
				if (left_sibling) {
					int16_t last = YuDbBPlusEntryRbTreeIteratorLast(&sibling->rb_tree); {
						if (!(last != YuDbBPlusEntryRbReferencer_InvalidId)) {
							*(int*)0 = 0;
						}
					}
					;
					YuDbBPlusElement* left_element = YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(sibling, last); {
						PageId temp = left_element->index.child_id;
						left_element->index.child_id = sibling->index.tail_child_id;
						sibling->index.tail_child_id = temp;
					}
					;
					YuDbBPlusElement* par_element = YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(parent, common_parent_element_id);
					par_element->index.child_id = left_element->index.child_id;
					YuDbBPlusEntryInsertElement(entry, parent, par_element, -1);
					left_element->index.child_id = sibling_entry_id;
					YuDbBPlusEntryInsertElement(parent, sibling, left_element, -1);
					YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(sibling, left_element);
					YuDbBPlusEntryDeleteElement(sibling, last);
					YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(parent, par_element);
					YuDbBPlusEntryDeleteElement(parent, common_parent_element_id);
				}
				else {
					int16_t first = YuDbBPlusEntryRbTreeIteratorFirst(&sibling->rb_tree); {
						if (!(first != YuDbBPlusEntryRbReferencer_InvalidId)) {
							*(int*)0 = 0;
						}
					}
					;
					YuDbBPlusElement* right_element = YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(sibling, first);
					YuDbBPlusElement* par_element = YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Reference(parent, common_parent_element_id);
					par_element->index.child_id = right_element->index.child_id; {
						PageId temp = par_element->index.child_id;
						par_element->index.child_id = entry->index.tail_child_id;
						entry->index.tail_child_id = temp;
					}
					;
					YuDbBPlusEntryInsertElement(entry, parent, par_element, -1);
					right_element->index.child_id = cur_pos->entry_id;
					YuDbBPlusEntryInsertElement(parent, sibling, right_element, -1);
					YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(sibling, right_element);
					YuDbBPlusEntryDeleteElement(sibling, first);
					YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(parent, par_element);
					YuDbBPlusEntryDeleteElement(parent, common_parent_element_id);
				}
			}
			break;
		}
		if (left_sibling) {
			YuDbBPlusEntryMerge(tree, sibling, sibling_entry_id, entry, cur_pos->entry_id, parent, common_parent_element_id);
			entry = ((void*)0);
		}
		else {
			YuDbBPlusEntryMerge(tree, entry, cur_pos->entry_id, sibling, sibling_entry_id, parent, common_parent_element_id);
			sibling = ((void*)0);
		}
		delete_up = 1;
	} while (0);
	if (parent) {
		YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(tree, parent);
	}
	if (sibling) {
		YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(tree, sibling);
	}
	if (entry) {
		YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_Dereference(tree, entry);
	}
	if (delete_up) {
		return YuDbBPlusTreeDeleteElement(tree, cursor);
	}
	return success;
}
void YuDbBPlusTreeInit(YuDbBPlusTree* tree) {
	tree->root_id = YuDbBPlusEntryCreate(tree, kBPlusEntryLeaf);
}
_Bool YuDbBPlusTreeFind(YuDbBPlusTree* tree, YuDbKey* key) {
	YuDbBPlusCursor cursor;
	BPlusCursorStatus status = YuDbBPlusCursorFirst(tree, &cursor, key);
	while (status == kBPlusCursorNext) {
		status = YuDbBPlusCursorNext(tree, &cursor, key);
	}
	return status == kBPlusCursorEq;
}
_Bool YuDbBPlusTreeInsert(YuDbBPlusTree* tree, YuDbBPlusLeafElement* element) {
	YuDbBPlusCursor cursor;
	BPlusCursorStatus status = YuDbBPlusCursorFirst(tree, &cursor, &element->key);
	while (status == kBPlusCursorNext) {
		status = YuDbBPlusCursorNext(tree, &cursor, &element->key);
	}
	_Bool success = YuDbBPlusTreeInsertElement(tree, &cursor, ((void*)0), (YuDbBPlusElement*)element, (-1), -1);
	YuDbBPlusCursorRelease(tree, &cursor);
	return success;
}
_Bool YuDbBPlusTreeDelete(YuDbBPlusTree* tree, YuDbKey* key) {
	YuDbBPlusCursor cursor;
	BPlusCursorStatus status = YuDbBPlusCursorFirst(tree, &cursor, key);
	while (status == kBPlusCursorNext) {
		status = YuDbBPlusCursorNext(tree, &cursor, key);
	}
	if (status == kBPlusCursorNe) {
		return 0;
	}
	_Bool success = YuDbBPlusTreeDeleteElement(tree, &cursor);
	return success;
}
#endif

int16_t YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR_CreateBySize(YuDbBPlusEntry* entry, int32_t size) {
	BucketEntry* bucket_entry = BPlusEntryToBucketEntry(entry);

	if (bucket_entry->info.alloc_size + size <= bucket_entry->info.page_size && YuDbBPlusEntryFreeListGetMaxFreeBlockSize(&bucket_entry->info.free_list, 0) < size) {
		// 需要进行碎片整理
		BucketEntry* temp = (BucketEntry*)MemoryAlloc(bucket_entry->info.page_size);

		size_t bp_entry_size;
		if (entry->type == kBPlusEntryIndex) {
			bp_entry_size = sizeof(YuDbBPlusEntry) - sizeof(YuDbBPlusLeafEntry) + sizeof(YuDbBPlusIndexEntry);
		}
		else {
			bp_entry_size = sizeof(YuDbBPlusEntry) - sizeof(YuDbBPlusIndexEntry) + sizeof(YuDbBPlusLeafEntry);
		}

		BucketEntryInit(temp, bp_entry_size, bucket_entry->info.page_size);
		temp->info.last_write_tx_id = bucket_entry->info.last_write_tx_id;

		// 创建临时entry，重新插入，最后复制回当前entry
		int16_t element_id = YuDbBPlusEntryRbTreeIteratorLast(&entry->rb_tree);
		while (element_id != YuDbBPlusEntryRbReferencer_InvalidId) {
			int16_t next_elemeng_id = YuDbBPlusEntryRbTreeIteratorPrev(&entry->rb_tree, element_id);
			YuDbBPlusElement* element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(entry, element_id);
			YuDbBPlusEntryInsertElement(temp, entry, element, YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_InvalidId);
			YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(entry, element);
			YuDbBPlusEntryDeleteElement(entry, element_id);
			element_id = next_elemeng_id;
		}

		memcpy(bucket_entry, temp, bucket_entry->info.page_size);
		MemoryFree(temp);
	}
	  assert(bucket_entry->info.alloc_size <= bucket_entry->info.page_size);

	  assert(size >= sizeof(YuDbBPlusEntryFreeBlockEntry));
	int16_t offset = YuDbBPlusEntryFreeListAlloc(&bucket_entry->info.free_list, 0, &size);
	bucket_entry->info.alloc_size += size;
	  assert(bucket_entry->info.alloc_size + YuDbBPlusEntryFreeListGetFreeBlockSize(&bucket_entry->info.free_list, 0) == bucket_entry->info.page_size - 2);
	return offset;
}




static PageId BucketEntryCopy(Bucket* bucket, BucketEntry* entry, PageId entry_pgid) {
	Tx* tx = BPlusTreeToTx(&bucket->bp_tree);
	PageId copy_pgid = PagerAlloc(&tx->db->pager, true, 1);
	if (copy_pgid == kPageInvalidId) {
		return kPageInvalidId;
	}
	BucketEntry* copy_entry = (BucketEntry*)PagerReference(&tx->db->pager, copy_pgid);
	memcpy(copy_entry, entry, tx->db->pager.page_size);
	copy_entry->info.last_write_tx_id = tx->meta_info.txid;
	//if (copy_entry->bp_entry.type == kBPlusEntryLeaf) {
	//	// 若存在叶子链表则无法进行写时复制(需要拷贝整条链)，故不支持叶子链表的连接
	//	PageId prev_id = entry->bp_entry.leaf.list_entry.prev;
	//	PageId next_id = entry->bp_entry.leaf.list_entry.next;
	//	YuDbBPlusEntry* next_entry = YUDB_BUCKET_BPLUS_REFERENCER_Reference(&bucket->bp_tree, next_id);
	//	YuDbBPlusEntry* prev_entry = YUDB_BUCKET_BPLUS_REFERENCER_Reference(&bucket->bp_tree, prev_id);
	//	YuDbBPlusLeafListReplaceEntry(&bucket->bp_tree.leaf_list, entry_pgid, copy_pgid);

	//	assert(prev_entry->type == kBPlusEntryLeaf);
	//	assert(next_entry->type == kBPlusEntryLeaf);

	//	PagerMarkDirty(&tx->db->pager, next_entry);
	//	PagerMarkDirty(&tx->db->pager, prev_entry);
	//	YUDB_BUCKET_BPLUS_REFERENCER_Dereference(&bucket->bp_tree, next_entry);
	//	YUDB_BUCKET_BPLUS_REFERENCER_Dereference(&bucket->bp_tree, prev_entry);
	//}
	PagerMarkDirty(&tx->db->pager, copy_entry);
	PagerDereference(&tx->db->pager, copy_entry);
	return copy_pgid;
}

/*
key最大只支持1页以内的长度
*/
void BucketInit(YuDb* db, Bucket* bucket) {
	BucketEntryInfo info;
	uint32_t bucket_entry_head_size = BPlusEntryGetHeadSize((uintptr_t)&info + sizeof(info));
	uint32_t bplus_entry_head_size = sizeof(YuDbBPlusEntry) - max(sizeof(YuDbBPlusLeafEntry), sizeof(YuDbBPlusIndexEntry));
	uint32_t head_size = (bucket_entry_head_size + bplus_entry_head_size + sizeof(YuDbBPlusIndexEntry));
	uint32_t index_m = (db->pager.page_size - head_size) / sizeof(YuDbBPlusIndexElement) + 1;

	head_size = head_size - sizeof(YuDbBPlusIndexEntry) + sizeof(YuDbBPlusLeafEntry);
	uint32_t leaf_m = (db->pager.page_size - head_size) / sizeof(YuDbBPlusLeafElement) + 1;

    YuDbBPlusTreeInit(&bucket->bp_tree);
}

bool BucketPut(Bucket* bucket, void* key_buf, int16_t key_size, void* value_buf, size_t value_size) {
	Tx* tx = BPlusTreeToTx(&bucket->bp_tree);
	if (tx->type != kTxReadWrite) {
		return false;
	}
	YuDbBPlusTree* tree = &tx->meta_info.bucket.bp_tree;

	YuDbBPlusLeafElement element;
	element.key.type = kDataMemory;
	element.key.mem_buf.is_value = false;
	element.value.type = kDataMemory;
	element.value.mem_buf.is_value = true;
	g_key.mem_ptr = key_buf;
	g_key.size = key_size;
	g_value.mem_ptr = value_buf;
	g_value.size = value_size;

    YuDbBPlusCursor cursor;
    BPlusCursorStatus status = YuDbBPlusCursorFirst(tree, &cursor, &element.key);
	bool success = true;
	// 进行页面路径的写时复制
    do  {
		YuDbBPlusElementPos* cur = YuDbBPlusCursorCur(tree, &cursor);
		BucketEntry* entry = (BucketEntry*)PagerReference(&tx->db->pager, cur->entry_id);
		if (entry->info.last_write_tx_id != tx->meta_info.txid) {		// 当前事务创建/修改的页面不需要重复cow
			PageId copy_id = BucketEntryCopy(&tx->meta_info.bucket, entry, cur->entry_id);
			if (copy_id == kPageInvalidId) {
				success = false;
				break;
			}
			PagerDereference(&tx->db->pager, BucketEntryToBPlusEntry(entry));
			PagerPending(&tx->db->pager, tx, cur->entry_id);

			// 游标回溯的pgid修改为拷贝的节点
			cur->entry_id = copy_id;

			// 需要修改上层的节点的元素指向拷贝的节点
			YuDbBPlusElementPos* up = YuDbBPlusCursorUp(tree, &cursor);
			if (up) {
				BucketEntry* up_entry = (BucketEntry*)PagerReference(&tx->db->pager, up->entry_id);
				YuDbBPlusElementSetChildId(BucketEntryToBPlusEntry(up_entry), up->element_id, copy_id);
				PagerDereference(&tx->db->pager, up_entry);
				YuDbBPlusCursorDown(tree, &cursor);
			}
			else {
				tx->meta_info.bucket.bp_tree.root_id = copy_id;
			}
		}
		else {
			PagerDereference(&tx->db->pager, entry);
		}
		
		if (status != kBPlusCursorNext) {
			break;
		}
        status = YuDbBPlusCursorNext(tree, &cursor, &element.key);
	} while (true);
	if (success == false) {
		return false;
	}
    success = YuDbBPlusTreeInsertElement(tree, &cursor, NULL, (YuDbBPlusElement*)&element, YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_InvalidId, YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_InvalidId);
    YuDbBPlusCursorRelease(tree, &cursor);
    return success;
}

bool BucketFind(Bucket* bucket, void* key_buf, int16_t key_size) {
	YuDbKey key;
	key.mem_buf.is_value = false;
	key.type = kDataMemory;
	g_key.mem_ptr = key_buf;
	g_key.size = key_size;
	return YuDbBPlusTreeFind(&bucket->bp_tree, &key);
}


//
//
//void BPlusEntryDelete(Tx* tx, PageId pgid) {
//	BPlusEntry* entry = BPlusEntryGet(tx, pgid);
//	// wal模式时最后持久化的pending不能释放，要想个办法
//	if (entry->last_write_tx_id == tx->meta_info.txid) {
//		BPlusEntryDereference(tx, entry);
//		PagerFree(&tx->db->pager, pgid, 1);
//	}
//	else {
//		BPlusEntryDereference(tx, entry);
//		TxPendingListEntry* free_list_entry = (TxPendingListEntry*)RbTreeFindEntryByKey(&tx->db->tx_manager.pending_page_list, &tx->meta_info.txid);
//		PagerPending(&tx->db->pager, pgid, 1, free_list_entry->first_pending_pgid);
//		free_list_entry->first_pending_pgid = pgid;
//	}
//}
//


//void* GetDataBuf(Tx* tx, Data* data, void** data_buf, size_t* data_size) {
//	Bucket* bucket = (Bucket*)tx;
//	if (data->block.type == kDataBlock) {
//		void* page = PagerGet(&tx->db->pager, data->block.pgid);
//		*data_buf = (void*)((uintptr_t)page + (data->block.offset << 2));
//		*data_size = data->block.size;
//		return page;
//	}
//	else if (data->embed.type == kDataEmbed) {
//		*data_buf = data->embed.data;
//		*data_size = data->embed.size;
//	}
//	else if (data->each.type == kDataEach) {
//		*data_buf = NULL;
//		*data_size = data->each.size;
//		// 独立页面返回数据大小，要求调用ReadData提供缓冲区读取(允许分段)
//	}
//	else {		// data->memory.type == kDataMemory
//		MemoryData* mem_data = (MemoryData*)(data->memory.mem_data << 2);
//		*data_buf = mem_data->buf;
//		*data_size = mem_data->size;
//	}
//	return NULL;
//}
//
//static void SetDataBuf(Tx* tx, BPlusEntry* entry, Data* data, void* data_buf, size_t data_size) {
//	if (data_size <= sizeof(data->embed.data)) {
//		// 可以内嵌
//		data->embed.type = kDataEmbed;
//		data->embed.size = data_size;
//		memcpy(data->embed.data, data_buf, data_size);
//	}
//	else if (data_size <= sizeof(data->embed.data)) {
//		// 可以申请块来存放
//		// OverflowPageBlockAlloc(bucket, );
//		data->block.type = kDataBlock;
//		data->block.size = data_size;
//
//	}
//	else {
//		// 需要单独使用一个或多个连续页面存放
//		uint32_t page_count = data_size / tx->db->pager.page_size;
//		if (data_size % tx->db->pager.page_size) {
//			page_count++;
//		}
//		PageId pgid = PagerAlloc(&tx->db->pager, true, page_count);
//		PagerWrite(&tx->db->pager, pgid, data_buf, page_count);
//		data->each.type = kDataEach;
//		data->each.pgid = pgid;
//		data->each.size = data_size;
//	}
//}
//
//void BPlusElementSet(Tx* tx, BPlusEntry* entry, int i, BPlusElement* element) {
//	Key* key = NULL;
//	Value* value = NULL;
//	if (entry->type == kBPlusEntryLeaf) {
//		if (element->leaf.key.memory.type == kDataMemory) {
//			key = &entry->leaf.element[i].key;
//			value = &entry->leaf.element[i].value;
//		}
//		else {
//			entry->leaf.element[i] = element->leaf;
//		}
//	}
//	else if (entry->type == kBPlusEntryIndex) {
//		if (element->index.key.memory.type == kDataMemory) {
//			key = &entry->index.element[i].key;
//		}
//		else {
//			entry->index.element[i] = element->index;
//		}
//	}
//	if (key) {
//		if (value) {
//			MemoryData* mem_data = (MemoryData*)(element->leaf.key.memory.mem_data << 2);
//			SetDataBuf(tx, entry, key, mem_data->buf, mem_data->size);
//			mem_data = (MemoryData*)(element->leaf.value.memory.mem_data << 2);
//			SetDataBuf(tx, entry, value, mem_data->buf, mem_data->size);
//		}
//		else {
//			MemoryData* mem_data = (MemoryData*)(element->index.key.memory.mem_data << 2);
//			SetDataBuf(tx, entry, key, mem_data->buf, mem_data->size);
//		}
//	}
//}
//
//ptrdiff_t BPlusKeyCmp(Tx* tx, const Key* key1, const Key* key2) {
//	size_t key1_size, key2_size;
//	void* key1_buf, * key2_buf;
//	void* cache1 = GetDataBuf(tx, key1, &key1_buf, &key1_size);
//	void* cache2 = GetDataBuf(tx, key2, &key2_buf, &key2_size);
//	ptrdiff_t res = MemoryCmpR2(key1_buf, key1_size, key2_buf, key2_size);
//	if (cache1) {
//		PagerDereference(&tx->db->pager, cache1);
//	}
//	if (cache2) {
//		PagerDereference(&tx->db->pager, cache2);
//	}
//	return res;
//}
//
