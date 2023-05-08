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

	int64_t count = 64650;



	//for (int i = 0; i < count; i++) {
	//	for (int i = 0; i < 1023; i+^+) {
	//		printf("%d\t", Free1TableAlloc_((Free1Table_*)table, 4));
	//	}
	//}
	
	int seed =11323;
	seed = GetTickCount();
	srand(seed);
	const int qqq = 1000;
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

	//l = GetTickCount64();
	//for (int i = 0; i < count; i++) {
	//	for (int j = 0; j < qqq; j++) {
	//		arr[j] = PagerAlloc(&db->pager, false, 1);
	//	}
	//	for (int j = 0; j < qqq; j++) {
	//		PagerFree(&db->pager, arr[j], 1);
	//	}
	//}
	//printf("read: %dms", (int)(GetTickCount64() - l));


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
			//printf("%d    ", GetBucketCount(&tx));
		}

		if (i == 0xf591) {
			printf("1??");
			
			//continue;
		}
		//CacheLruListHashTableIterator iter;
		//CacheLruHashEntry* hash_entry = CacheLruListHashTableIteratorFirst(&db->pager.cacher.lru_list.hash_table, &iter);
		//if (hash_entry) {

		//	hash_entry = CacheLruListHashTableIteratorNext(&db->pager.cacher.lru_list.hash_table, &iter);
		//}
		int n = 0;
		//CacheLruListHashTableIterator itera;
		CacheLruListHashTable* table = &db->pager.cacher.lru_list.hash_table;
		CacheLruListHashLinkRbObj rb_obj;
		rb_obj.table = table;
		bool aa11e = false;
		rb_obj.rb_tree = table->bucket.obj_arr[0x11e].rb_tree;
		int32_t id = CacheLruListHashLinkRbTreeIteratorFirst(&rb_obj.rb_tree);
		while (id != -1) {
			//printf("id:%d\n", id);
			if (id == 1) {
				aa11e = true;
			}
			id = CacheLruListHashLinkRbTreeIteratorNext(&rb_obj.rb_tree, id);
		}

		rb_obj.rb_tree = table->bucket.obj_arr[0x169].rb_tree;
		id = CacheLruListHashLinkRbTreeIteratorFirst(&rb_obj.rb_tree);
		bool aa169 = false;
		while (id != -1) {
			if (id == 1) {
				aa169 = true;
			}
			//printf("id:%d\n", id);

			id = CacheLruListHashLinkRbTreeIteratorNext(&rb_obj.rb_tree, id);
		}
		if (aa11e && aa169) {
			printf("??");
		}


		/*for (int j = 0; j < table->bucket.count; j++) {
			rb_obj.rb_tree = table->bucket.obj_arr[j].rb_tree;
			int32_t id = CacheLruListHashLinkRbTreeIteratorFirst(&rb_obj.rb_tree);
			while (id != -1) {
				if (id == 1) {
					n++;
					if (n == 2) {
						printf("??");
					}
				}
				id = CacheLruListHashLinkRbTreeIteratorNext(&rb_obj.rb_tree, id);
			}
		}
		*/


		
		YuDbBPlusLeafListHead* head = &tx.meta_info.bucket.bp_tree.leaf_list;
		PageId pgid = YuDbBPlusLeafListFirst(head);
		bool aa = false;
		while (pgid != -1) {
			BucketEntry* entry = (BucketEntry*)PagerReference(&db->pager, pgid);
			if (pgid == 0Xa) {
				if (i == 0xf591) {

					aa = true;
				}
				printf("%x ", i);
			}
			if (pgid == 0x7 || pgid == 0x9 || pgid == 0xb) {
				if (i == 0xf591) {
					aa = true;
				}
			}
			PagerDereference(&db->pager, entry);
			pgid = YuDbBPlusLeafListNext(head, pgid);
		}
		if (i > 0x1fc && aa == false) {
			aa = false;
		}

		aa11e = false;
		rb_obj.rb_tree = table->bucket.obj_arr[0x11e].rb_tree;
		id = CacheLruListHashLinkRbTreeIteratorFirst(&rb_obj.rb_tree);
		while (id != -1) {
			//printf("id:%d\n", id);
			if (id == 1) {
				aa11e = true;
			}
			id = CacheLruListHashLinkRbTreeIteratorNext(&rb_obj.rb_tree, id);
		}

		rb_obj.rb_tree = table->bucket.obj_arr[0x169].rb_tree;
		id = CacheLruListHashLinkRbTreeIteratorFirst(&rb_obj.rb_tree);
		aa169 = false;
		while (id != -1) {
			if (id == 1) {
				aa169 = true;
			}
			//printf("id:%d\n", id);

			id = CacheLruListHashLinkRbTreeIteratorNext(&rb_obj.rb_tree, id);
		}
		if (aa11e && aa169) {
			printf("??");
		}


		if (!BucketPut(&tx.meta_info.bucket, (void*)&iter.first, 4, (void*)&iter.second, 4)) {
			printf("NOW!");
		}


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
	
	for (auto& iter : map) {
		++i;
		if (m == 0) {
			TxBegin(db, &tx, kTxReadOnly);
		}
		if (!BucketFind(&tx.meta_info.bucket, (void*)&iter.first, 4)) {
			printf("NOR!, %d  %d  ", iter.first, iter.second);
		}
		//YuDbBPlusLeafListHead* head = &tx.meta_info.bucket.bp_tree.leaf_list;
		//PageId pgid = YuDbBPlusLeafListFirst(head);

		//while (pgid != -1) {
		//	BucketEntry* entry = (BucketEntry*)PagerReference(&db->pager, pgid);
		//	if (pgid == 0Xa) {
		//		aaaa();
		//	}
		//	PagerDereference(&db->pager, entry);
		//	pgid = YuDbBPlusLeafListNext(head, pgid);
		//}

		if (m == 0) {
			TxCommit(&tx);
		}
	}

	if (m == 1) {
		TxBegin(db, &tx, kTxReadWrite);
	}


	i = 0;
	BucketEntry* entry = (BucketEntry*)PagerReference(&db->pager, 9);

	for (auto& iter : map) {
		i++;
		if (i == 0xFF) {
			printf("2??");
			
			//continue;
		}
		if (m == 0) {
			TxBegin(db, &tx, kTxReadWrite);
			//printf("%d    ", GetBucketCount(&tx));
		}

		
		
		if (!BucketPut(&tx.meta_info.bucket, (void*)&iter.first, 4, (void*)&iter.second, 4)) {
			printf("NOW!");
		}

		
		

		//BucketEntry* entry = (BucketEntry*)PagerReference(&tx.db->pager, tx.meta_info.bucket.bp_tree.root_id, '0');
		//PrintBucket(&tx, &entry->bp_entry, 0, 0);
		//printf("\n\n\n\n\n");

		//
		//printf("\n\n\n\n");
		if (m == 0) {
			TxCommit(&tx);
		}
	}
	PagerDereference(&db->pager, entry);
	if (m == 1) {
		TxCommit(&tx);
	}

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



