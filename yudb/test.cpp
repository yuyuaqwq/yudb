#include <map>

#include <stdlib.h>
#include <stdio.h>

#include <yudb/yudb.h>
#include <yudb/transaction.h>

extern "C" PageId GetDataBuf(Tx* tx, Data* data, void** data_buf, size_t* data_size);


void PrintBucket(Tx* tx, YuDbBPlusEntry* entry, int Level, int pos) {
	if (!entry) return;
	Pager* pager = &tx->db->pager;
	char* empty = (char*)malloc(Level * 8 + 1);
	memset(empty, ' ', Level * 8);
	empty[Level * 8] = 0;

	if (entry->type == 1) {
		int16_t id = YuDbBPlusEntryRbTreeIteratorLast(&entry->rb_tree);
		//PrintRB(&entry->rb_tree, entry->rb_tree.root, 0, true);
		for (int i = entry->element_count - 1; i >= 0; i--) {
			printf("%sleaf:::key:%d\n%sLevel:%d\n%sParent:%d\n\n", empty, entry->leaf.element_space.obj_arr[id].key, empty, Level, empty/*, pos ? ((BPlusEntry*)PageGet(tree, entry->, 1))->indexElement[pos].key : 0*/);
			id = YuDbBPlusEntryRbTreeIteratorPrev(&entry->rb_tree, id);
		}
		free(empty);
		return;
	}

	//PrintRB(&entry->rb_tree, entry->rb_tree.root, 0, false);
	int16_t id = YuDbBPlusEntryRbTreeIteratorLast(&entry->rb_tree);
	for (int i = entry->element_count; i >= 0; i--) {
		if (i == entry->element_count) {
			PrintBucket(tx, &((BucketEntry*)PagerReference(pager, entry->index.tail_child_id))->bp_entry, Level + 1, i - 1);
			continue;
		}
		printf("%sindex:::key:%d\n%sLevel:%d\n%sParent:%d\n\n", empty, entry->index.element_space.obj_arr[id].key, empty, Level, empty/*, entry->parentId != kPageInvalidId ? ((BPlusEntry*)PageGet(tree, entry->parentId))->indexElement[pos].key: 0*/);
		PrintBucket(tx, &((BucketEntry*)PagerReference(pager, (PageId)entry->index.element_space.obj_arr[id].child_id))->bp_entry, Level + 1, i);
		id = YuDbBPlusEntryRbTreeIteratorPrev(&entry->rb_tree, id);
	}
	free(empty);
}

//void PrintRB(YuDbBPlusEntryRbTree* tree, int16_t entry_id, int Level, bool index) {
//	if (entry_id == -1) return;
//	YuDbBPlusEntryRbEntry* entry = YuDbBPlusEntryRbReferencer_Reference(tree, entry_id);
//	PrintRB(tree, entry->right, Level + 1, index);
//
//	//print
//	const char* str = "Not";
//	if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, entry) != -1) {
//		str = (YuDbBPlusEntryRbReferencer_Reference(tree, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, entry))->right == entry_id ? "Right" : "Left");
//	}
//	int aaa = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, entry);
//	const char* color = aaa == 1 ? "Red" : "Black";
//
//	char* empty = (char*)malloc(Level * 8 + 1);
//	memset(empty, ' ', Level * 8);
//	empty[Level * 8] = 0;
//
//	int parentKey = 0;
//	if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, entry) != -1) {
//		if (index) {
//			parentKey = ((YuDbBPlusIndexElement*)YuDbBPlusEntryRbReferencer_Reference(tree, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, entry)))->key;
//		}
//		else {
//			parentKey = ((YuDbBPlusLeafElement*)YuDbBPlusEntryRbReferencer_Reference(tree, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, entry)))->key;
//		}
//	}
//
//	printf("%skey:%x\n%sLevel:%d\n%sParent.%s:%x\n%scolor:%s\n\n", empty, *YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetKey(tree, entry), empty, Level, empty, str, parentKey, empty, color);
//
//	free(empty);
//
//	PrintRB(tree, entry->left, Level + 1, index);
//}


//
//int GetBucketCount(Tx* tx) {
//	int count = 0;
//	PageId head_leaf = tx->meta_info.bucket.leaf_list_first;
//	PageId leaf = head_leaf;
//	do {
//		BPlusEntry* entry = BPlusEntryGet(tx, leaf);
//		count += entry->element_count;
//		
//		PageId next_leaf = entry->leaf.list_entry.next;
//		BPlusEntryDereference(tx, entry);
//		leaf = next_leaf;
//	} while (leaf != head_leaf);
//	return count;
//}


#include <yudb/free_table.h>

extern "C" void aaaa() {

}

long long l;
extern "C" int i = 0;

__forceinline PageId* CacheLruHashEntryAccessor_GetKey(CacheLruListHashTable* table, CacheLruHashEntry* hash_entry) {
	return (&((CacheInfo*)((uintptr_t)(hash_entry->lru_entry) - ((uintptr_t) & (((CacheInfo*)0)->lru_entry))))->pgid);
}

int main() {
	int r = 0;
	int m = 0;

	int64_t count = 200000;



	//for (int i = 0; i < count; i++) {
	//	for (int i = 0; i < 1023; i+^+) {
	//		printf("%d\t", Free1TableAlloc_((Free1Table_*)table, 4));
	//	}
	//}
	

	// 现在的页面分配器还有一个小问题，释放小id(6~1023)后，还是可能先分配大id(1026~2047)，页面利用率下降
	int seed =11323;
	seed = GetTickCount();
	srand(seed);
	const int qqq = count;
	int* arr = (int*)malloc(qqq * 4);
	//for (int i = 0; i < qqq; i++) {
	//	arr[i] = i;
	//}
	//for (int i = 0; i < qqq; i++) {
	//	int j = rand() % qqq;
	//	int temp = arr[j];
	//	int k = rand() % qqq;
	//	arr[j] = arr[k];
	//	arr[k] = temp;
	//}

	Config config;
	config.page_size = 4096;
	config.cacher_page_count = 1024;
	config.sync_mode = kConfigSyncNormal;
	config.update_mode = kConfigUpdateInPlace;
	config.hotspot_queue_full_percentage = 50;
	config.wal_max_page_count = 8;
	config.wal_max_tx_count = 100000;
	config.wal_write_thread_disk_drop_interval = 100;
	YuDb* db = YuDbOpen("Z:\\test.ydb", &config);

	/*l = GetTickCount64();
	for (int i = 0; i < count; i++) {
		arr[i] = PagerAlloc(&db->pager, false, 1);
		if (arr[i] == 0 || arr[i] == -1) {
			printf("??");
		}
	}
	for (int i = 0; i < count; i++) {
		PagerFree(&db->pager, arr[i], 1);
	}
	printf("read: %dms", (int)(GetTickCount64() - l));
*/

	PageId id;
	Tx tx;
	l = GetTickCount64();

	//db->update_mode = kYuDbUpdateInPlace;
	

	
	//while (true) {
	//	TxBegin(db, &tx, kTxReadWrite);
	//	
	//	id = PagerAlloc(&db->pager, true, 1);
	//	CacheId cid = CacherGetIdByBuf(&db->pager.cacher, PagerGet(&db->pager, id));
	//	printf("%d\t%d\t\t", id, cid);
	//	if (id == kPageInvalidId) {
	//		break;
	//	}
	//	TxCommit(&tx);
	//}
	//l = GetTickCount64() - l;
	//printf("%dms\n", (int)l);

	
	srand(14134);//GetTickCount()

	std::map<int, int> map;
	for (int i = 0; i < count; i++) {
		int j = rand() << 16 | rand();
		map.insert(std::make_pair(j, j));
	}


	l = GetTickCount64();
	
	
	if (m == 1) {
		TxBegin(db, &tx, kTxReadWrite);
	}
	i = 0;
	for (auto& iter : map) {
		i++;
		if (m == 0) {
			TxBegin(db, &tx, kTxReadWrite);
		}


		int n = 0;
	

		
		if (!BucketPut(&tx.meta_info.bucket, (void*)&iter.first, 4, (void*)&iter.second, 4)) {
			printf("NOW!");
		}
		printf("\n");

		//BucketEntry* entry = (BucketEntry*)PagerReference(&tx.db->pager, tx.meta_info.bucket.bp_tree.root_id, '0');
		//PrintBucket(&tx, &entry->bp_entry, 0, 0);
		//printf("\n\n\n\n\n");

		//
		//printf("\n\n\n\n");
		if (m == 0) {
			TxCommit(&tx);
		}
	}
	if (m == 1) {
		TxCommit(&tx);
	}
	

	//if (m == 1) {
	//	TxBegin(db, &tx, kTxReadWrite);
	//}
	//i = 0;
	//for (auto& iter : map) {
	//	i++;
	//	
	//	if (m == 0) {
	//		TxBegin(db, &tx, kTxReadWrite);
	//		//printf("%d    ", GetBucketCount(&tx));
	//	}

	//
	//	if (!BucketPut(&tx.meta_info.bucket, (void*)&iter.first, 4, (void*)&iter.second, 4)) {
	//		printf("NOW!");
	//	}

	//	
	//	if (m == 0) {
	//		TxCommit(&tx);
	//	}
	//}
	//if (m == 1) {
	//	TxCommit(&tx);
	//}


	l = GetTickCount64() - l;
	if (l == 0) {
		l = 1;
	}
	printf("write: %dms, %dtps, %dns/op\n", (int)l, (int)(count * 1000 / l), (int)((l * 1000 * 1000) / count));

	l = GetTickCount64();

	if (m == 1) {
		TxBegin(db, &tx, kTxReadOnly);
	}
	// PrintBucket(&tx, tx.meta_info.bucket.root_id, 0, 0);
	//printf("%d", GetBucketCount(&tx));
	i = 0;
	for (auto& iter : map) {
		++i;
		if (m == 0) {
			TxBegin(db, &tx, kTxReadOnly);
		}
		if (!BucketFind(&tx.meta_info.bucket, (void*)&iter.first, 4)) {
			printf("NOR!, %d  %d  ", iter.first, iter.second);
		}
		if (m == 0) {
			TxCommit(&tx);
		}
	}
	if (m == 1) {
		TxCommit(&tx);
	}
	
	// TxPut(&tx, "dwadad", 6, "123456", 6);
	l = GetTickCount64() - l;
	if (l == 0) {
		l = 1;
	}
	printf("read: %dms, %dtps, %dns/op", (int)l, (int)(count * 1000 / l), (int)((l * 1000 * 1000) / count));
	// YuDbPut(db, );
	// YuDbPut(db, "dwadad", 6, "123456", 6);
	printf("\n");


	YuDbClose(db);

	system("pause");
}



