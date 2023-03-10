#include <map>

#include <stdlib.h>
#include <stdio.h>

#include <yudb/yudb.h>
#include <yudb/transaction.h>

extern "C" PageId GetDataBuf(Tx* tx, Data* data, void** data_buf, size_t* data_size);

void PrintBucket(Tx* tx, PageId pgid, int Level, int pos) {
	BPlusEntry* entry = BPlusEntryGet(tx, pgid);
	if (!entry) return;
	char* empty = (char*)malloc(Level * 8 + 1);
	memset(empty, ' ', Level * 8);
	empty[Level * 8] = 0;

	if (entry->type == 1) {
		for (int i = entry->element_count - 1; i >= 0; i--) {
			void* key;
			size_t size;
			GetDataBuf(tx, &entry->leaf.element[i].key, &key, &size);
			printf("%sleaf:::key:%d\n%sLevel:%d\n%sParent:%d\n\n", empty, *(uint32_t*)key, empty, Level, empty/*, pos ? ((BPlusEntry*)PageGet(tree, entry->, 1))->indexElement[pos].key : 0*/);
		}
		free(empty);
		return;
	}



	for (int i = entry->element_count; i >= 0; i--) {
		if (i == entry->element_count) {
			PrintBucket(tx, entry->index.tail_child_id, Level + 1, i - 1);
			continue;
		}
		void* key;
		size_t size;
		GetDataBuf(tx, &entry->index.element[i].key, &key, &size);
		printf("%sindex:::key:%d\n%sLevel:%d\n%sParent:%d\n\n", empty, *(uint32_t*)key, empty, Level, empty/*, entry->parentId != kPageInvalidId ? ((BPlusEntry*)PageGet(tree, entry->parentId))->indexElement[pos].key: 0*/);
		PrintBucket(tx, entry->index.element[i].child_id, Level + 1, i);
	}
	free(empty);
	BPlusEntryDereference(tx, pgid);
}

int GetBucketCount(Tx* tx) {
	int count = 0;
	PageId head_leaf = tx->meta_info.bucket.leaf_list_first;
	PageId leaf = head_leaf;
	do {
		BPlusEntry* entry = BPlusEntryGet(tx, leaf);
		count += entry->element_count;
		
		PageId next_leaf = entry->leaf.list_entry.next;
		BPlusEntryDereference(tx, leaf);
		leaf = next_leaf;
	} while (leaf != head_leaf);
	return count;
}

long long l;
int main() {
	int r = 0;
	int m = 1;

	// 还有bug，重复开事务写会膨胀体积，好像是没有回收pending？

	int count = 100000;

	YuDb* db = YuDbOpen("Z:\\test.ydb");
	PageId id;
	Tx tx;
	l = GetTickCount64();

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
	

	for (auto& iter : map) {
		if (m == 0) {
			TxBegin(db, &tx, kTxReadWrite);
			//printf("%d    ", GetBucketCount(&tx));
		}
		//if (entry->index.tail_child_id == entry->index.element[entry->element_count-1].child_id) {
		//	PrintBucket(&tx, tx.meta_info.bucket.root_id, 0, 0);
		//	printf("\n\n\n\n");
		//}
		
		if (!BucketInsert(&tx, (void*)&iter.first, 4, (void*)&iter.second, 4)) {
			printf("NOW!");
		}
		//
		//printf("\n\n\n\n");
		if (m == 0) {
			TxCommit(&tx);
		}
	}
	if (m == 1) {
		TxCommit(&tx);
	}




	l = GetTickCount64() - l;
	if (l == 0) {
		l = 1;
	}
	printf("write: %dms, %dtps, %dus/op\n", (int)l, (int)(count * 1000 / l), (int)((l * 1000) / count));

	l = GetTickCount64();

	if (m == 1) {
		TxBegin(db, &tx, kTxReadOnly);
	}
	// PrintBucket(&tx, tx.meta_info.bucket.root_id, 0, 0);
	//printf("%d", GetBucketCount(&tx));
	for (auto& iter : map) {
		if (m == 0) {
			TxBegin(db, &tx, kTxReadOnly);
		}
		if (!BucketFind(&tx, (void*)&iter.first, 4)) {
			printf("NOR!, %d  %d  ", iter.first, iter.second);
		}
		if (m == 0) {
			TxBegin(db, &tx, kTxReadOnly);
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
	system("pause");
}



