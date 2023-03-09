#include <yudb/bucket.h>

#include <yudb/yudb.h>
#include <yudb/pager.h>


BPlusTree* BPlusTreeGet(Tx* tx) {
    return (BPlusTree*)&((Tx*)tx)->meta_info.bucket;
}

BPlusEntry* BPlusEntryGet(Tx* tx, PageId pgid) {
    return PagerGet(&tx->db->pager, pgid);
}

void BPlusEntryDereference(Tx* tx, PageId pgid) {
    PagerDereference(&tx->db->pager, pgid);
}

PageId BPlusEntryCreate(Tx* tx, BPlusEntryType type) {
    BPlusTree* tree = BPlusTreeGet(tx);
    PageId entry_id = PagerAlloc(&tx->db->pager, true, 1);
	PagerMarkDirty(&tx->db->pager, entry_id);
    BPlusEntry* entry = BPlusEntryGet(tx, entry_id);
    entry->type = type;
    entry->element_count = 0;
    entry->last_write_tx_id = tx->meta_info.txid;
    BPlusEntryDereference(tx, entry_id);
    return entry_id;
}

void BPlusEntryDelete(Tx* tx, PageId pgid) {
	BPlusEntry* entry = BPlusEntryGet(tx, pgid);
	if (entry->last_write_tx_id == tx->meta_info.txid) {
		BPlusEntryDereference(tx, pgid);
		PagerFree(&tx->db->pager, pgid, 1);
	}
	else {
		BPlusEntryDereference(tx, pgid);
		TxPendingListEntry* free_list_entry = (TxPendingListEntry*)RbTreeFindEntryByKey(&tx->db->tx_manager.pending_page_list, &tx->meta_info.txid);
		PagerPending(&tx->db->pager, pgid, 1, free_list_entry->first_pgid);
		free_list_entry->first_pgid = pgid;
	}
}

PageId BPlusEntryCopy(Tx* tx, BPlusEntry* entry) {
    PageId copy_pgid = BPlusEntryCreate(tx, entry->type);
	if (copy_pgid == kPageInvalidId) {
		return kPageInvalidId;
	}
    BPlusEntry* copy = BPlusEntryGet(tx, copy_pgid);
    memcpy(copy, entry, tx->db->pager.page_size);
    copy->last_write_tx_id = tx->meta_info.txid;
	if (copy->type == kBPlusEntryLeaf) {
		// 当前是叶子节点，需要处理一下叶子节点的前后连接链表
		PageId prev_pgid = entry->leaf.list_entry.prev;
		BPlusEntry* prev = BPlusEntryGet(tx, prev_pgid);
		PageId next_pgid = entry->leaf.list_entry.next;
		BPlusEntry* next = BPlusEntryGet(tx, next_pgid);
		prev->leaf.list_entry.next = copy_pgid;
		next->leaf.list_entry.prev = copy_pgid;
		PagerMarkDirty(&tx->db->pager, prev_pgid);
		PagerMarkDirty(&tx->db->pager, next_pgid);
		BPlusEntryDereference(tx, prev_pgid);
		BPlusEntryDereference(tx, next_pgid);
	}
    BPlusEntryDereference(tx, copy_pgid);
    return copy_pgid;
}

void BPlusElementSetChildId(Tx* tx, BPlusEntry* index, int i, PageId id);


PageId GetDataBuf(Tx* tx, Data* data, void** data_buf, size_t* data_size) {
	Bucket* bucket = (Bucket*)tx;
	if (data->block.type == kDataBlock) {
		void* page = PagerGet(&tx->db->pager, data->block.pgid, false);
		*data_buf = (void*)((uintptr_t)page + (data->block.offset << 2));
		*data_size = data->block.size;
		return data->block.pgid;
	}
	else if (data->embed.type == kDataEmbed) {
		*data_buf = data->embed.data;
		*data_size = data->embed.size;
	}
	else if (data->each.type == kDataEach) {
		*data_buf = NULL;
		*data_size = data->each.size;
		// 独立页面返回数据大小，要求调用ReadData提供缓冲区读取(允许分段)
	}
	else {		// data->memory.type == kDataMemory
		MemoryData* mem_data = (MemoryData*)(data->memory.mem_data << 2);
		*data_buf = mem_data->buf;
		*data_size = mem_data->size;
	}
	return kPageInvalidId;
}

static void SetDataBuf(Tx* tx, BPlusEntry* entry, Data* data, void* data_buf, size_t data_size) {
	if (data_size <= sizeof(data->embed.data)) {
		// 可以内嵌
		data->embed.type = kDataEmbed;
		data->embed.size = data_size;
		memcpy(data->embed.data, data_buf, data_size);
	}
	else if (data_size <= sizeof(data->embed.data)) {
		// 可以申请块来存放
		// OverflowPageBlockAlloc(bucket, );
		data->block.type = kDataBlock;
		data->block.size = data_size;

	}
	else {
		// 需要单独使用一个或多个连续页面存放
		uint32_t page_count = data_size / tx->db->pager.page_size;
		if (data_size % tx->db->pager.page_size) {
			page_count++;
		}
		PageId pgid = PagerAlloc(&tx->db->pager, true, page_count);
		PagerWrite(&tx->db, pgid, data_buf, page_count);
		data->each.type = kDataEach;
		data->each.pgid = pgid;
		data->each.size = data_size;
	}
}

void BPlusElementSet(Tx* tx, BPlusEntry* entry, int i, BPlusElement* element) {
	Key* key = NULL;
	Value* value = NULL;
	if (entry->type == kBPlusEntryLeaf) {
		if (element->leaf.key.memory.type == kDataMemory) {
			key = &entry->leaf.element[i].key;
			value = &entry->leaf.element[i].value;
		}
		else {
			entry->leaf.element[i] = element->leaf;
		}
	}
	else if (entry->type == kBPlusEntryIndex) {
		if (element->index.key.memory.type == kDataMemory) {
			key = &entry->index.element[i].key;
		}
		else {
			entry->index.element[i] = element->index;
		}
	}
	if (key) {
		if (value) {
			MemoryData* mem_data = (MemoryData*)(element->leaf.key.memory.mem_data << 2);
			SetDataBuf(tx, entry, key, mem_data->buf, mem_data->size);
			mem_data = (MemoryData*)(element->leaf.value.memory.mem_data << 2);
			SetDataBuf(tx, entry, value, mem_data->buf, mem_data->size);
		}
		else {
			MemoryData* mem_data = (MemoryData*)(element->index.key.memory.mem_data << 2);
			SetDataBuf(tx, entry, key, mem_data->buf, mem_data->size);
		}
	}
}


ptrdiff_t BPlusKeyCmp(Tx* tx, const Key* key1, const Key* key2) {
	PageId key1_pgid, key2_pgid;
	size_t key1_size, key2_size;
	void* key1_buf, * key2_buf;
	key1_pgid = GetDataBuf(tx, key1, &key1_buf, &key1_size);
	key2_pgid = GetDataBuf(tx, key2, &key2_buf, &key2_size);
	ptrdiff_t res = MemoryCmpR2(key1_buf, key1_size, key2_buf, key2_size);
	PagerDereference(&tx->db->pager, key1_pgid);
	PagerDereference(&tx->db->pager, key2_pgid);
	return res;
}




void BucketInit(YuDb* db, Tx* tx) {
	uint32_t index_m = (db->pager.page_size - (sizeof(BPlusEntry) - max(sizeof(BPlusLeafEntry), sizeof(BPlusIndexEntry)) + sizeof(BPlusIndexEntry))) / sizeof(BPlusIndexElement) + 1;
	uint32_t leaf_m = (db->pager.page_size - (sizeof(BPlusEntry) - max(sizeof(BPlusLeafEntry), sizeof(BPlusIndexEntry)) + sizeof(BPlusLeafEntry))) / sizeof(BPlusLeafElement) + 1;
    BPlusTreeInit(tx, index_m, leaf_m);
}

// key最大只支持1页以内的长度
bool BucketInsert(Tx* tx, void* key_buf, int16_t key_size, void* value_buf, size_t value_size) {
	if (tx->type != kTxReadWrite) {
		return false;
	}
	MemoryData key;
	key.buf = key_buf;
	key.size = key_size;
	MemoryData value;
	value.buf = value_buf;
	value.size = value_size;
	BPlusLeafElement element;
	element.key.memory.type = kDataMemory;
	element.value.memory.type = kDataMemory;
	element.key.memory.mem_data = ((uintptr_t)&key) >> 2;
	element.value.memory.mem_data = ((uintptr_t)&value) >> 2;

    BPlusCursor cursor;
    BPlusCursorStatus status = BPlusCursorFirst(tx, &cursor, &element.key);
	bool success = true;
    do  {
		BPlusElementPos* cur = BPlusCursorCur(tx, &cursor);
		BPlusEntry* entry = BPlusEntryGet(tx, cur->entry_id);
		if (tx->meta_info.txid != entry->last_write_tx_id) {
			PageId copy_id = BPlusEntryCopy(tx, entry, cur->entry_id);
			if (copy_id == kPageInvalidId) {
				success = false;
				break;
			}
			BPlusEntryDereference(tx, cur->entry_id);
			BPlusEntryDelete(tx, cur->entry_id);

			cur->entry_id = copy_id;		// 回溯的id修改为拷贝的节点

			// 需要修改上层的节点的元素指向拷贝的节点
			BPlusElementPos* up = BPlusCursorUp(tx, &cursor);
			if (up) {
				BPlusEntry* up_entry = BPlusEntryGet(tx, up->entry_id);
				BPlusElementSetChildId(tx, up_entry, up->element_idx, copy_id);
				BPlusEntryDereference(tx, up->entry_id);
				BPlusCursorDown(tx, &cursor);
			}
			else {
				tx->meta_info.bucket.root_id = copy_id;
			}
		}
		else {
			BPlusEntryDereference(tx, cur->entry_id);
		}
		
		if (status != kBPlusCursorNext) {
			break;
		}
        status = BPlusCursorNext(tx, &cursor, &element.key);
	} while (true);
	if (success == false) {
		return false;
	}
    success = BPlusInsertEntry(tx, &cursor, &element);
    BPlusCursorRelease(tx, &cursor);
    return success;
}

bool BucketFind(Tx* tx, void* key_buf, int16_t key_size) {
	MemoryData key_data;
	key_data.buf = key_buf;
	key_data.size = key_size;
	Key key;
	key.memory.type = kDataMemory;
	key.memory.mem_data = ((uintptr_t)&key_data) >> 2;;
	return BPlusTreeFind(tx, &key);
}




#include <CUtils/container/bplus_tree.c>
