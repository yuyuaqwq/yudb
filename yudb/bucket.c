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
	block->count = element_count;
}
int16_t YuDbBPlusEntryFreeListAlloc(YuDbBPlusEntryFreeList* head, int16_t list_order, int16_t count) {
	YuDbBPlusEntryFreeBlockEntry* prev_block = (YuDbBPlusEntryFreeBlockEntry*)(&head->first_block[list_order]);
	int16_t free_offset = head->first_block[list_order];
	while (free_offset != (-1)) {
		YuDbBPlusEntryFreeBlockEntry* block = (YuDbBPlusEntryFreeBlockEntry*)(&head->obj_arr[free_offset]);
		if (block->count > count) {
			YuDbBPlusEntryFreeBlockEntry* new_block = (YuDbBPlusEntryFreeBlockEntry*)(&head->obj_arr[free_offset + count]);
			new_block->next_block_offset = block->next_block_offset;
			  assert(new_block->next_block_offset != 0);
			new_block->count = block->count - count;
			prev_block->next_block_offset += count;
			return free_offset;
		}
		else if (block->count == count) {
			prev_block->next_block_offset = block->next_block_offset;
			return free_offset;
		}
		free_offset = block->next_block_offset;
		prev_block = block;
	}
	;
	return (-1);
}
void YuDbBPlusEntryFreeListFree(YuDbBPlusEntryFreeList* head, int16_t list_order, int16_t free_offset, int16_t count) {
	int16_t cur_offset = head->first_block[list_order];
	YuDbBPlusEntryFreeBlockEntry* prev_block = (YuDbBPlusEntryFreeBlockEntry*)(&head->first_block[list_order]);
	YuDbBPlusEntryFreeBlockEntry* cur_block;
	YuDbBPlusEntryFreeBlockEntry* free_prev_prev_block = ((void*)0), * free_next_prev_block = ((void*)0);
	YuDbBPlusEntryFreeBlockEntry* free_prev_block = ((void*)0), * free_next_block = ((void*)0);
	while (cur_offset != (-1)) {
		cur_block = (YuDbBPlusEntryFreeBlockEntry*)(&head->obj_arr[cur_offset]);
		if (!free_next_block && free_offset + count == cur_offset) {
			if (free_prev_block) {
				free_prev_prev_block->next_block_offset = free_prev_block->next_block_offset;
			}
			count += cur_block->count;
			int16_t next_offset = cur_block->next_block_offset;
			cur_block = (YuDbBPlusEntryFreeBlockEntry*)(&head->obj_arr[free_offset]);
			cur_block->count = count;
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
			count += cur_block->count;
			cur_block->count = count;
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
		cur_block->count = count;
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
	entry->info.alloc_size = BucketEntryGetHeadSize(entry) + bp_entry_size;		// 分配大小把BucketEntry头部长度算上，而free_list分配的偏移是不算上的
	entry->info.page_size = page_size;
	YuDbBPlusEntryFreeListAlloc(&entry->info.free_list, 0, bp_entry_size);
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
	BucketEntry* buckey_entry = BPlusEntryToBucketEntry(entry);
	int16_t max_free_size = YuDbBPlusEntryFreeListGetMaxFreeBlockSize(&buckey_entry->info.free_list, 0);
	return max_free_size;
}

int32_t YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR_GetFillRate(YuDbBPlusTree* tree, YuDbBPlusEntry* entry) {
	Tx* tx = BPlusTreeToTx(tree);
	BucketEntry* bucket_entry = BPlusEntryToBucketEntry(entry);
	return bucket_entry->info.alloc_size;
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


forceinline void BlockRelease(BucketEntry* entry, int16_t element_id, size_t size) {
	entry->info.alloc_size -= size;
	YuDbBPlusEntryFreeListFree(&entry->info.free_list, 0, element_id, size);
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
		data_buf = ((MemoryData*)data)->mem_ptr;
		*size = ((MemoryData*)data)->size;
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
		return ((MemoryData*)data)->size;
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
	  assert(bucket_entry->info.alloc_size + YuDbBPlusEntryFreeListGetFreeBlockSize(&bucket_entry->info.free_list, 0) == bucket_entry->info.page_size);
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
		src_data_buf = ((MemoryData*)src)->mem_ptr;
		size = ((MemoryData*)src)->size;
		if (((MemoryData*)src)->size <= sizeof(dst->inline_.data)) {
			dst_data_buf = dst->inline_.data;
			dst->inline_.size = size;
			dst->type = kDataInline;
		} else {
			size = ((MemoryData*)src)->size;
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
				YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR_Release(src_entry, size);
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
	//YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_SetData(dst_entry, &element->leaf.value, value);
	element->leaf.value.type = kDataInline;
	element->leaf.value.inline_.size = 0;
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


//#define AAA
#ifndef AAA
CUTILS_CONTAINER_BPLUS_TREE_DEFINE(YuDb, CUTILS_CONTAINER_BPLUS_TREE_LEAF_LINK_MODE_NOT_LINK, 
	PageId, int16_t, YuDbKey, YuDbValue, CUTILS_OBJECT_ALLOCATOR_DEFALUT, YUDB_BUCKET_BPLUS_ENTRY_ALLOCATOR,
	YUDB_BUCKET_BPLUS_ENTRY_REFERENCER, YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR, YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR,
	YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER, YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR, 
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR, YUDB_BUCKET_BPLUS_COMPARER, 8)
#else
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
	bucket_entry->info.alloc_size += size;
	  assert(bucket_entry->info.alloc_size <= bucket_entry->info.page_size);

	int16_t offset = YuDbBPlusEntryFreeListAlloc(&bucket_entry->info.free_list, 0, size);
	  assert(bucket_entry->info.alloc_size + YuDbBPlusEntryFreeListGetFreeBlockSize(&bucket_entry->info.free_list, 0) == bucket_entry->info.page_size);
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

	YuDbBPlusLeafElement element[2];	// 这里会比较烦，用数组预留空间吧
	MemoryData* key = (MemoryData*)&element[0].key;
	key->type = kDataMemory;
	key->mem_ptr = value_buf;
	key->size = value_size;

    YuDbBPlusCursor cursor;
    BPlusCursorStatus status = YuDbBPlusCursorFirst(tree, &cursor, (YuDbKey*)key);
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
        status = YuDbBPlusCursorNext(tree, &cursor, (YuDbKey*)key);
	} while (true);
	if (success == false) {
		return false;
	}
    success = YuDbBPlusTreeInsertElement(tree, &cursor, NULL, element, YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_InvalidId, YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_InvalidId);
    YuDbBPlusCursorRelease(tree, &cursor);
    return success;
}

bool BucketFind(Bucket* bucket, void* key_buf, int16_t key_size) {
	MemoryData key_data;
	key_data.type = kDataMemory;
	key_data.mem_ptr = key_buf;
	key_data.size = key_size;
	return YuDbBPlusTreeFind(&bucket->bp_tree, (YuDbKey*) & key_data);
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
