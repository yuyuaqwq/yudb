#include <stdlib.h>
#include <stdio.h>

#include <yudb/yudb.h>
#include <yudb/transaction.h>



PageId GetDataBuf(Tx* tx, Data* data, void** data_buf, size_t* data_size);

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
}




int main() {
	int r = 0;
	
	int count = 200000;

	YuDb* db = YuDbOpen("Z:\\test.ydb");
	srand(14134);//GetTickCount()

	long long l = GetTickCount64();
	Tx tx;
	
	
	for (int i = 0; i < count; i++) {
		TxBegin(db, &tx, kTxReadWrite);
		if (i == 449) {
			printf("23");
		}
		//if (entry->index.tail_child_id == entry->index.element[entry->element_count-1].child_id) {
		//	PrintBucket(&tx, tx.meta_info.bucket.root_id, 0, 0);
		//	printf("\n\n\n\n");
		//}
		int j = rand() << 16 | rand();
		BucketInsert(&tx, &j, 4, &j, 4);
		//
		//printf("\n\n\n\n");
		TxCommit(&tx);
	}
	
	l = GetTickCount64() - l;
	printf("write: %dms, %dtps, %dus/op\n", (int)l, (int)(count * 1000 / l), (int)((l * 1000) / count));

	l = GetTickCount64();
	srand(14134);//GetTickCount()
	TxBegin(db, &tx, kTxReadOnly);
	for (int i = 0; i < count; i++) {
		
		int j = rand() << 16 | rand();
		if (!BucketFind(&tx, &j, 4)) {
			printf("NO!");
		}
		
	}
	TxCommit(&tx);
	// TxPut(&tx, "dwadad", 6, "123456", 6);
	l = GetTickCount64() - l;
	printf("read: %dms, %dtps, %dns/op", (int)l, (int)(count * 1000 / l), (int)((l * 1000 * 1000) / count));
	// YuDbPut(db, );
	// YuDbPut(db, "dwadad", 6, "123456", 6);
	printf("\n");
	system("pause");
}



