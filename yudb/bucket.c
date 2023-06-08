#include <yudb/bucket.h>

#include <yudb/yudb.h>
#include <yudb/pager.h>

static inline Tx* BPlusTreeToTx(YuDbBPlusTree* tree) {
	Bucket* bucket = ObjectGetFromField(tree, Bucket, bp_tree);
	MetaInfo* meta_info = ObjectGetFromField(bucket, MetaInfo, bucket);
	Tx* tx = ObjectGetFromField(meta_info, Tx, meta_info);
	return tx;
}

static inline uint32_t GetBucketHeadSize(YuDbBPlusTree* tree) {
	Tx* tx = BPlusTreeToTx(tree);
	return sizeof(BucketEntry) + tx->db->pager.data_pool_count * sizeof(PageId);
}

static inline YuDbBPlusEntry* BucketEntryToBPlusEntry(YuDbBPlusTree* tree, BucketEntry* entry) {
	return (YuDbBPlusEntry*)((uintptr_t)entry + GetBucketHeadSize(tree));
}

static inline YuDbBPlusEntry* BPlusEntryToBucketEntry(YuDbBPlusTree* tree, YuDbBPlusEntry* entry) {
	return (YuDbBPlusEntry*)((uintptr_t)entry - GetBucketHeadSize(tree));
}


/*
* B+树访问器
*/
forceinline int32_t* YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetKey(YuDbBPlusEntryRbTree* tree, YuDbBPlusEntryRbBsEntry* bs_entry) {
	if (((YuDbBPlusEntry*)tree)->type == kBPlusEntryLeaf) {
		return &((YuDbBPlusLeafElement*)bs_entry)->key;
	}
	else {
		return &((YuDbBPlusIndexElement*)bs_entry)->key;
	}
}
/*
* B+树分配器
*/
inline PageId YUDB_BUCKET_BPLUS_ALLOCATOR_CreateBySize(YuDbBPlusTree* tree, size_t size) {
	Tx* tx = BPlusTreeToTx(tree);
	PageId pgid = PagerAlloc(&tx->db->pager, true, 1);
	BucketEntry* entry = (BucketEntry*)PagerReference(&tx->db->pager, pgid);
	PagerMarkDirty(&tx->db->pager, entry);
	for (int32_t i = 0; i < tx->db->pager.data_pool_count; i++) {
		entry->first_data_pool[i] = kPageInvalidId;
	}
	entry->last_write_tx_id = tx->meta_info.txid;
	PagerDereference(&tx->db->pager, entry);
	return pgid;
}
inline void YUDB_BUCKET_BPLUS_ALLOCATOR_Release(YuDbBPlusTree* tree, PageId pgid) {
	Tx* tx = BPlusTreeToTx(tree);
	PagerPending(&tx->db->pager, tx, pgid);
}
/*
* B+树引用器
*/
#define YUDB_BUCKET_BPLUS_REFERENCER_InvalidId -1
inline YuDbBPlusEntry* YUDB_BUCKET_BPLUS_REFERENCER_Reference(YuDbBPlusTree* tree, PageId pgid) {
	Tx* tx = BPlusTreeToTx(tree);
	BucketEntry* entry = (BucketEntry*)PagerReference(&tx->db->pager, pgid);
	return BucketEntryToBPlusEntry(tree, entry);
}
inline void YUDB_BUCKET_BPLUS_REFERENCER_Dereference(YuDbBPlusTree* tree, YuDbBPlusEntry* bp_entry) {
	Tx* tx = BPlusTreeToTx(tree);
	BucketEntry* entry = BPlusEntryToBucketEntry(tree, bp_entry);
	PagerDereference(&tx->db->pager, entry);
}


#define YUDB_BUCKET_BPLUS_REFERENCER YUDB_BUCKET_BPLUS_REFERENCER
#define YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR
#define YUDB_BUCKET_BPLUS_ALLOCATOR YUDB_BUCKET_BPLUS_ALLOCATOR
CUTILS_CONTAINER_BPLUS_TREE_DEFINE(YuDb, CUTILS_CONTAINER_BPLUS_TREE_LEAF_LINK_MODE_NOT_LINK, PageId, int16_t, int32_t, int32_t, CUTILS_OBJECT_ALLOCATOR_DEFALUT, YUDB_BUCKET_BPLUS_ALLOCATOR, YUDB_BUCKET_BPLUS_REFERENCER, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR, CUTILS_OBJECT_COMPARER_DEFALUT)



static PageId BucketEntryCopy(Bucket* bucket, BucketEntry* entry, PageId entry_pgid) {
	Tx* tx = BPlusTreeToTx(&bucket->bp_tree);
	PageId copy_pgid = PagerAlloc(&tx->db->pager, true, 1);
	if (copy_pgid == kPageInvalidId) {
		return kPageInvalidId;
	}
	BucketEntry* copy_entry = (BucketEntry*)PagerReference(&tx->db->pager, copy_pgid);
	memcpy(copy_entry, entry, tx->db->pager.page_size);
	copy_entry->last_write_tx_id = tx->meta_info.txid;
	//if (copy_entry->bp_entry.type == kBPlusEntryLeaf) {
	//	// 叶子链表无法进行写时复制(需要拷贝整条链)，故不支持叶子链表的连接
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
	uint32_t bucket_entry_head_size = GetBucketHeadSize(&bucket->bp_tree);
	uint32_t bplus_entry_head_size = sizeof(YuDbBPlusEntry) - max(sizeof(YuDbBPlusLeafEntry), sizeof(YuDbBPlusIndexEntry));
	uint32_t head_size = (bucket_entry_head_size + bplus_entry_head_size + sizeof(YuDbBPlusIndexEntry));
	uint32_t index_m = (db->pager.page_size - head_size) / sizeof(YuDbBPlusIndexElement) + 1;

	head_size = head_size - sizeof(YuDbBPlusIndexEntry) + sizeof(YuDbBPlusLeafEntry);
	uint32_t leaf_m = (db->pager.page_size - head_size) / sizeof(YuDbBPlusLeafElement) + 1;

    YuDbBPlusTreeInit(&bucket->bp_tree, index_m, leaf_m);
}

bool BucketPut(Bucket* bucket, void* key_buf, int16_t key_size, void* value_buf, size_t value_size) {
	Tx* tx = BPlusTreeToTx(&bucket->bp_tree);
	if (tx->type != kTxReadWrite) {
		return false;
	}
	YuDbBPlusTree* tree = &tx->meta_info.bucket.bp_tree;

	//MemoryData key;
	//key.buf = key_buf;
	//key.size = key_size;
	//MemoryData value;
	//value.buf = value_buf;
	//value.size = value_size;
	YuDbBPlusLeafElement element;
	//element.key.memory.type = kDataMemory;
	//element.value.memory.type = kDataMemory;
	//element.key.memory.mem_data = ((uintptr_t)&key) >> 2;
	//element.value.memory.mem_data = ((uintptr_t)&value) >> 2;
	element.key = *(int32_t*)value_buf;

    YuDbBPlusCursor cursor;
    BPlusCursorStatus status = YuDbBPlusCursorFirst(tree, &cursor, &element.key);
	bool success = true;
	// 进行页面路径的写时复制
    do  {
		YuDbBPlusElementPos* cur = YuDbBPlusCursorCur(tree, &cursor);
		BucketEntry* entry = (BucketEntry*)PagerReference(&tx->db->pager, cur->entry_id);
		
		if (entry->last_write_tx_id != tx->meta_info.txid) {		// 当前事务创建/修改的页面不需要重复cow
			PageId copy_id = BucketEntryCopy(&tx->meta_info.bucket, entry, cur->entry_id);
			if (copy_id == kPageInvalidId) {
				success = false;
				break;
			}
			PagerDereference(&tx->db->pager, BucketEntryToBPlusEntry(&bucket->bp_tree, entry));
			PagerPending(&tx->db->pager, tx, cur->entry_id);

			// 游标回溯的pgid修改为拷贝的节点
			cur->entry_id = copy_id;

			// 需要修改上层的节点的元素指向拷贝的节点
			YuDbBPlusElementPos* up = YuDbBPlusCursorUp(tree, &cursor);
			if (up) {
				BucketEntry* up_entry = (BucketEntry*)PagerReference(&tx->db->pager, up->entry_id);
				YuDbBPlusElementSetChildId(tree, BucketEntryToBPlusEntry(&bucket->bp_tree, up_entry), up->element_id, copy_id);
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
    success = YuDbBPlusTreeInsertElement(tree, &cursor, &element);
    YuDbBPlusCursorRelease(tree, &cursor);
    return success;
}

bool BucketFind(Bucket* bucket, void* key_buf, int16_t key_size) {
	MemoryBuffer key_data;
	key_data.buf = key_buf;
	key_data.size = key_size;
	//Key key;
	//key.memory.type = kDataMemory;
	//key.memory.mem_data = ((uintptr_t)&key_data) >> 2;;
	return YuDbBPlusTreeFind(&bucket->bp_tree, key_buf);
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
