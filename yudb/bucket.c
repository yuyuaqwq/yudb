#include <yudb/bucket.h>

#include <yudb/yudb.h>
#include <yudb/pager.h>



static forceinline Tx* BPlusTreeToTx(YuDbBPlusTree* tree) {
	Bucket* bucket = ObjectGetFromField(tree, Bucket, bp_tree);
	MetaInfo* meta_info = ObjectGetFromField(bucket, MetaInfo, bucket);
	Tx* tx = ObjectGetFromField(meta_info, Tx, meta_info);
	return tx;
}

static forceinline uint32_t BucketEntryGetHeadSize(BucketEntry* entry) {
	return sizeof(BucketEntryInfo);
}

static forceinline uint32_t BPlusEntryGetHeadSize(BucketEntry* entry) {
	return sizeof(BucketEntryInfo);
}

static forceinline YuDbBPlusEntry* BucketEntryToBPlusEntry(BucketEntry* entry) {
	return (YuDbBPlusEntry*)((uintptr_t)entry + BucketEntryGetHeadSize(entry));
}

static forceinline BucketEntry* BPlusEntryToBucketEntry(YuDbBPlusEntry* entry) {
	return (BucketEntry*)((uintptr_t)entry - BPlusEntryGetHeadSize(NULL));
}


/*
* B+树Entry分配器
*/
static void BucketEntryInit(BucketEntry* entry, int16_t bp_entry_size, PageSize page_size) {
	DataPoolInit(&entry->info.data_pool, page_size - BucketEntryGetHeadSize(entry));
	
	// BPlusEntry头部占用
	entry->info.page_size = page_size;
	entry->info.alloc_size = BucketEntryGetHeadSize(entry);		// 分配大小把BucketEntry头部长度算上，而free_list分配的偏移是不算上的
	DataPoolAlloc(&entry->info.data_pool, bp_entry_size);
	
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
	int16_t max_free_size = DataPoolFreeListGetMaxFreeBlockSize(&bucket_entry->info.data_pool.free_list, 0);
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
	BucketEntry* temp_entry = (BucketEntry*)tx->db->pager.temp_page;
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
	BucketEntry* bucket_entry = BPlusEntryToBucketEntry(entry);
	return (YuDbBPlusElement*)DataPoolGetBlock(&bucket_entry->info.data_pool, element_id);
}
forceinline void YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(YuDbBPlusEntry* entry, YuDbBPlusElement* element) {

}
#define YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER

/*
* B+树Element分配器
*/
int16_t YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR_CreateBySize(YuDbBPlusEntry* entry, int32_t size);
void YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR_Release(YuDbBPlusEntry* entry, int16_t element_id) {
	BucketEntry* bucket_entry = BPlusEntryToBucketEntry(entry);
	YuDbBPlusElement* element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(entry, element_id);
	//bucket_entry->info.alloc_size -= YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_GetNeedRate(entry, element);
	if (entry->type == kBPlusEntryIndex) {
		DataDescriptorReleaseExpand(&bucket_entry->info.data_pool, &element->index.key);
		DataPoolRelease(&bucket_entry->info.data_pool, element_id, sizeof(YuDbBPlusIndexElement));
	}
	else {
		DataDescriptorReleaseExpand(&bucket_entry->info.data_pool, &element->leaf.key);
		DataDescriptorReleaseExpand(&bucket_entry->info.data_pool, &element->leaf.value);
		DataPoolRelease(&bucket_entry->info.data_pool, element_id, sizeof(YuDbBPlusLeafElement));
	}
	YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(entry, element);
}
#define YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR

/*
* B+树Element访问器
*/
int32_t YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_GetNeedRate(YuDbBPlusEntry* dst_entry, YuDbBPlusEntry* src_entry, YuDbBPlusElement* element) {
	size_t size = 0;
	if (src_entry) {
		BucketEntry* src_bucket_entry = BPlusEntryToBucketEntry(src_entry);
		if (src_entry->type == kBPlusEntryIndex) {
			size += DataDescriptorGetExpandSize(&src_bucket_entry->info.data_pool, &element->index.key);
		}
		else {
			size += DataDescriptorGetExpandSize(&src_bucket_entry->info.data_pool, &element->leaf.key);
			size += DataDescriptorGetExpandSize(&src_bucket_entry->info.data_pool, &element->leaf.value);
		}
	}
	else {
		BucketEntry* dst_bucket_entry = BPlusEntryToBucketEntry(src_entry);
		if (dst_entry->type == kBPlusEntryIndex) {
			size += DataDescriptorGetExpandSize(&dst_bucket_entry->info.data_pool, &element->index.key);
		}
		else {
			size += DataDescriptorGetExpandSize(&dst_bucket_entry->info.data_pool, &element->leaf.key);
			size += DataDescriptorGetExpandSize(&dst_bucket_entry->info.data_pool, &element->leaf.value);
		}
	}
	size += dst_entry->type == kBPlusEntryIndex ? sizeof(YuDbBPlusIndexElement) : sizeof(YuDbBPlusLeafElement);
	size = size + (size % sizeof(DataPoolFreeBlockEntry) ? sizeof(DataPoolFreeBlockEntry) - size % sizeof(DataPoolFreeBlockEntry) : 0);
	return size;
}
void YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR_SetData(YuDbBPlusEntry* dst_entry, DataDescriptor* dst, YuDbBPlusEntry* src_entry, const DataDescriptor* src) {
	void* src_data_buf = NULL;
	void* dst_data_buf = NULL;
	size_t size = 1;
	DataType type = kDataMemory;

	// 先释放已有的扩展块
	BucketEntry* dst_bucket_entry = BPlusEntryToBucketEntry(dst_entry);
	DataDescriptorReleaseExpand(&dst_bucket_entry->info.data_pool, dst);

	if (src->type == kDataMemory) {
		MemoryData* data = src->mem_buf.is_value ? &g_value : &g_key;
		src_data_buf = (void*)data->mem_ptr;
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
		src_data_buf = (void*)src->inline_.data;
		size = src->inline_.size;
		dst_data_buf = dst->inline_.data;
		dst->inline_.size = size;
		dst->type = kDataInline;
	}
	else if (src->type == kDataBlock) {
		src_data_buf = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(src_entry, src->block.data_id);
		size = src->block.size;
	lable_dst_block:
		{
			int16_t data_id = DataPoolAlloc(&dst_bucket_entry->info.data_pool, size);
			dst_data_buf = DataPoolGetBlock(&dst_bucket_entry->info.data_pool, data_id);
			dst->block.size = size;
			dst->block.data_id = data_id;
			dst->type = kDataBlock;
		}
	}
	else {
		dst->type = src->type;
	}
	memcpy(dst_data_buf, src_data_buf, size);
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
forceinline int32_t YUDB_BUCKET_BPLUS_COMPARER_Subrrac(YuDbBPlusEntryRbTree* tree, YuDbKey* key1, YuDbKey* key2) {
	YuDbBPlusEntry* entry = ObjectGetFromField(tree, YuDbBPlusEntry, rb_tree);
	BucketEntry* bucket_entry = BPlusEntryToBucketEntry(entry);
	void* key1_data, *key2_data;
	int16_t key1_size, key2_size;
	key1_data = DataDescriptorParser(&bucket_entry->info.data_pool, key1, &key1_size);
	key2_data = DataDescriptorParser(&bucket_entry->info.data_pool, key2, &key2_size);
	ptrdiff_t res = 0;
	if (key1_size == key2_size) {
		res = MemoryCmp(key1_data, key2_data, key1_size);
	}
	else {
		res = key1_size - key2_size;
	}
	return res;
}
forceinline bool YUDB_BUCKET_BPLUS_COMPARER_Equal(YuDbBPlusEntryRbTree* tree, YuDbKey* key1, YuDbKey* key2) {
	return YUDB_BUCKET_BPLUS_COMPARER_Subrrac(tree, key1, key2) == 0;
}
forceinline bool YUDB_BUCKET_BPLUS_COMPARER_Greater(YuDbBPlusEntryRbTree* tree, YuDbKey* key1, YuDbKey* key2) {
	return YUDB_BUCKET_BPLUS_COMPARER_Subrrac(tree, key1, key2) > 0;
}
forceinline bool YUDB_BUCKET_BPLUS_COMPARER_Less(YuDbBPlusEntryRbTree* tree, YuDbKey* key1, YuDbKey* key2) {
	return YUDB_BUCKET_BPLUS_COMPARER_Subrrac(tree, key1, key2) < 0;
}
#define YUDB_BUCKET_BPLUS_COMPARER YUDB_BUCKET_BPLUS_COMPARER

//#define AAA
#ifndef AAA
CUTILS_CONTAINER_BPLUS_TREE_DEFINE(YuDb, CUTILS_CONTAINER_BPLUS_TREE_LEAF_LINK_MODE_NOT_LINK, 
	PageId, int16_t, YuDbKey, YuDbValue, CUTILS_OBJECT_ALLOCATOR_DEFALUT, YUDB_BUCKET_BPLUS_ENTRY_ALLOCATOR,
	YUDB_BUCKET_BPLUS_ENTRY_REFERENCER, YUDB_BUCKET_BPLUS_ENTRY_ACCESSOR, YUDB_BUCKET_BPLUS_ELEMENT_ACCESSOR,
	YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER, YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR, 
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR, YUDB_BUCKET_BPLUS_COMPARER, 32)
#else
#endif

int16_t YUDB_BUCKET_BPLUS_ELEMENT_ALLOCATOR_CreateBySize(YuDbBPlusEntry* entry, int32_t size) {
	BucketEntry* bucket_entry = BPlusEntryToBucketEntry(entry);

	if (bucket_entry->info.alloc_size + size <= bucket_entry->info.page_size && DataPoolFreeListGetMaxFreeBlockSize(&bucket_entry->info.data_pool.free_list, 0) < size) {
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
			YuDbBPlusEntryInsertElement(BucketEntryToBPlusEntry(temp), entry, element, YUDB_BUCKET_BPLUS_ENTRY_REFERENCER_InvalidId);
			YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(entry, element);
			YuDbBPlusEntryDeleteElement(entry, element_id);
			element_id = next_elemeng_id;
		}

		memcpy(bucket_entry, temp, bucket_entry->info.page_size);
		MemoryFree(temp);
	}
	  assert(bucket_entry->info.alloc_size <= bucket_entry->info.page_size);

	  assert(size >= sizeof(DataPoolFreeBlockEntry));
	int16_t element_id = DataPoolAlloc(&bucket_entry->info.data_pool, size);

	YuDbBPlusElement* element = YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(entry, element_id);
	memset(element, 0, size);
	YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Dereference(entry, element);

	int16_t free_size;
	return element_id;
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
	uint32_t bucket_entry_head_size = BPlusEntryGetHeadSize((BucketEntry*)((uintptr_t)&info + sizeof(info)));
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
	g_key.mem_ptr = (uintptr_t)key_buf;
	g_key.size = key_size;
	g_value.mem_ptr = (uintptr_t)value_buf;
	g_value.size = value_size;

    YuDbBPlusCursor cursor;
    BPlusCursorStatus status = YuDbBPlusCursorFirst(tree, &cursor, &element.key);
	bool success = true;
	// 进行页面路径的写时复制
    do  {
		YuDbBPlusElementPos* cur = YuDbBPlusCursorCur(tree, &cursor);
		BucketEntry* entry = (BucketEntry*)PagerReference(&tx->db->pager, cur->entry_id);
		PagerMarkDirty(&tx->db->pager, entry);
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
	g_key.mem_ptr = (uintptr_t)key_buf;
	g_key.size = key_size;
	return YuDbBPlusTreeFind(&bucket->bp_tree, &key);
}