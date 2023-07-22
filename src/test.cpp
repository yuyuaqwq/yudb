#include <map>
#include <vector>

#include <stdlib.h>
#include <stdio.h>

#include <yudb/yudb.h>
#include <yudb/transaction.h>

extern "C" PageId GetDataBuf(Tx* tx, DataDescriptor* data, void** data_buf, size_t* data_size);

extern "C" YuDbBPlusElement * YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(YuDbBPlusEntry * entry, int16_t element_id);
static inline PageCount BucketEntryGetHeadSize(BucketEntry* entry) {
  return sizeof(BucketEntryInfo);
}
static inline YuDbBPlusEntry* BucketEntryToBPlusEntry(BucketEntry* entry) {
  return (YuDbBPlusEntry*)((uintptr_t)entry + BucketEntryGetHeadSize(entry));
}

extern "C" void* DataDescriptorParser(DataPool * data_pool, DataDescriptor * data, int16_t * size);
//void PrintBucket(Tx* tx, YuDbBPlusEntry* entry, int Level, int pos) {
//  if (!entry) return;
//  Pager* pager = &tx->db->pager;
//  char* empty = (char*)malloc(Level * 8 + 1);
//  memset(empty, ' ', Level * 8);
//  empty[Level * 8] = 0;
//
//  if (entry->type == 1) {
//    int16_t id = YuDbBPlusEntryRbTreeIteratorLast(&entry->rb_tree);
//    //PrintRB(&entry->rb_tree, entry->rb_tree.root, 0, true);
//    for (int i = entry->element_count - 1; i >= 0; i--) {
//      
//      size_t size;
//      printf("%sleaf:::key:%x\n%sLevel:%d\n%sParent:%d\n\n", empty, *(uint32_t*)DataBlockParser(entry, &YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(entry, id)->leaf.key, size), empty, Level, empty/*, pos ? ((BPlusEntry*)PageGet(tree, entry->, 1))->indexElement[pos].key : 0*/);
//      id = YuDbBPlusEntryRbTreeIteratorPrev(&entry->rb_tree, id);
//    }
//    free(empty);
//    return;
//  }
//
//  //PrintRB(&entry->rb_tree, entry->rb_tree.root, 0, false);
//  int16_t id = YuDbBPlusEntryRbTreeIteratorLast(&entry->rb_tree);
//  for (int i = entry->element_count; i >= 0; i--) {
//    if (i == entry->element_count) {
//      BucketEntry* bucket_entry = (BucketEntry*)PagerReference(pager, entry->index.tail_child_id);
//      PrintBucket(tx, BucketEntryToBPlusEntry(bucket_entry), Level + 1, i - 1);
//      PagerDereference(pager, bucket_entry);
//      continue;
//    }
//    size_t size;
//    printf("%sindex:::key:%x\n%sLevel:%d\n%sParent:%d\n\n", empty, *(uint32_t*)DataBlockParser(entry, &YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(entry, id)->index.key, size), empty, Level, empty/*, entry->parentId != kPageInvalidId ? ((BPlusEntry*)PageGet(tree, entry->parentId))->indexElement[pos].key: 0*/);
//    BucketEntry* bucket_entry = (BucketEntry*)PagerReference(pager, (PageId)YUDB_BUCKET_BPLUS_ELEMENT_REFERENCER_Reference(entry, id)->index.child_id);
//    PrintBucket(tx, BucketEntryToBPlusEntry(bucket_entry), Level + 1, i);
//    PagerDereference(pager, bucket_entry);
//    id = YuDbBPlusEntryRbTreeIteratorPrev(&entry->rb_tree, id);
//  }
//  free(empty);
//}


#define LIBYUC_CONTINUE_RB_TREE_ACCESSOR_DEFALUT_GetKey(TREE, ENTRY) (&ObjectGetFromField((ENTRY), CacheInfo, dirty_entry)->pgid)
extern "C" void PrintRB(CacheRbTree * tree, CacheRbEntry * entry_id, int Level, bool index) {
  if (entry_id == NULL) return;
  CacheRbEntry* entry = entry_id;
  PrintRB(tree, entry->right, Level + 1, index);

  //print
  const char* str = "Not";
  if (LIBYUC_CONTINUE_RB_TREE_ACCESSOR_DEFALUT_GetParent(tree, entry) != NULL) {
    str = ((CacheRbEntry*)LIBYUC_CONTINUE_RB_TREE_ACCESSOR_DEFALUT_GetParent(tree, entry))->right == entry_id ? "Right" : "Left";
  }
  int aaa = LIBYUC_CONTINUE_RB_TREE_ACCESSOR_DEFALUT_GetColor(tree, entry);
  const char* color = aaa == 1 ? "Red" : "Black";

  char* empty = (char*)malloc(Level * 8 + 1);
  memset(empty, ' ', Level * 8);
  empty[Level * 8] = 0;

  PageId parentKey = 0;
  if (LIBYUC_CONTINUE_RB_TREE_ACCESSOR_DEFALUT_GetParent(tree, entry) != NULL) {
    parentKey = *LIBYUC_CONTINUE_RB_TREE_ACCESSOR_DEFALUT_GetKey(tree, entry);
  }

  printf("%skey:%d\n%sLevel:%d\n%sParent.%s:%x\n%scolor:%s\n\n", empty, *LIBYUC_CONTINUE_RB_TREE_ACCESSOR_DEFALUT_GetKey(tree, entry), empty, Level, empty, str, parentKey, empty, color);

  free(empty);

  PrintRB(tree, entry->left, Level + 1, index);
}


//
//int GetBucketCount(Tx* tx) {
//  int count = 0;
//  PageId head_leaf = tx->meta_info.bucket.leaf_list_first;
//  PageId leaf = head_leaf;
//  do {
//    BPlusEntry* entry = BPlusEntryGet(tx, leaf);
//    count += entry->element_count;
//    
//    PageId next_leaf = entry->leaf.list_entry.next;
//    BPlusEntryDereference(tx, entry);
//    leaf = next_leaf;
//  } while (leaf != head_leaf);
//  return count;
//}


#include <yudb/free_manager/free_manager.h>

extern "C" void aaaa() {

}

long long l;
extern "C" int i = 0;

__forceinline PageId* CacheLruHashEntryAccessor_GetKey(CacheHashListHashTable* table, CacheHashListHashEntry* hash_entry) {
  return (&((CacheInfo*)((uintptr_t)(hash_entry->hash_list_entry) - ((uintptr_t) & (((CacheInfo*)0)->lru_entry))))->pgid);
}

int main() {
  int r = 0;
  int m = 1;
  int w = 1;

  int64_t count = 1000;


  //for (int i = 0; i < count; i++) {
  //  for (int i = 0; i < 1023; i+^+) {
  //    printf("%d\t", Free1TableAlloc_((Free1Table_*)table, 4));
  //  }
  //}
  

  // 现在的页面分配器还有一个小问题，释放小id(6~1023)后，还是可能先分配大id(1026~2047)，页面复用率下降
  int seed = 316884890;
  //seed = GetTickCount();
  srand(seed);
  printf("seed:%d\n", seed);
  const int qqq = count;
  int* arr = (int*)malloc(qqq * 4);
  //for (int i = 0; i < qqq; i++) {
  //  arr[i] = i;
  //}
  //for (int i = 0; i < qqq; i++) {
  //  int j = rand() % qqq;
  //  int temp = arr[j];
  //  int k = rand() % qqq;
  //  arr[j] = arr[k];
  //  arr[k] = temp;
  //}

  Config config;
  config.page_size = 4096;
  config.cacher_page_count = 10240;
  config.sync_mode = kConfigSyncNormal;
  config.update_mode = kConfigUpdateInPlace;
  config.hotspot_queue_full_percentage = 100;
  config.wal_max_page_count = 8;
  config.wal_max_tx_count = 100000;
  config.wal_write_thread_disk_drop_interval = 100;
  YuDb* db = YuDbOpen("Z:\\test.ydb", &config);

  FreeManagerTest(&db->pager.free_manager);

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
  //  TxBegin(db, &tx, kTxReadWrite);
  //  
  //  id = PagerAlloc(&db->pager, true, 1);
  //  CacheId cid = CacherGetIdByBuf(&db->pager.cacher, PagerGet(&db->pager, id));
  //  printf("%d\t%d\t\t", id, cid);
  //  if (id == kPageInvalidId) {
  //    break;
  //  }
  //  TxCommit(&tx);
  //}
  //l = GetTickCount64() - l;
  //printf("%dms\n", (int)l);

  
  //srand(14134);//GetTickCount()

  //std::map<uint64_t, uint64_t> map;
  std::vector<uint64_t> aaa(count);

  for (int i = 0; i < count; i++) {
    aaa[i] = (uint64_t)rand() << 48 | (uint64_t)rand() << 32 | (uint64_t)rand() << 16 | (uint64_t)rand();
    //uint64_t j = (uint64_t)rand() << 48 | (uint64_t)rand() << 32 | (uint64_t)rand() << 16 | (uint64_t)rand();
    //j &= 0xffffffff;
    //map.insert(std::make_pair(j, j));
  }

  /*for (int i = 0; i < count; i++) {
    std::swap(aaa[i], aaa[(rand() << 16 |rand()) & count]);
  }*/


  


  if (w) {
    l = GetTickCount64();


    if (m == 1) {
      TxBegin(db, &tx, kTxReadWrite);
    }
    i = 0;
    for (auto& iter : aaa) {
      i++;
      if (m == 0) {
        TxBegin(db, &tx, kTxReadWrite);
      }
      //int64_t kk = 2531101099859663433;
      //if ( i>= 668 && !BucketFind(&tx.meta_info.bucket, (void*)&kk, 4)) {// 
      //  printf("??");
      //}

      //PageId pgid = 28;
      //auto id = CacheHashListGet(&tx.db->pager.cacher.lru_list, &pgid, false);
      //if (i > 4176 && id == NULL) {
      //  printf("??");
      //}
      //PrintRB(&tx.db->pager.cacher.dirty_tree, tx.db->pager.cacher.dirty_tree.root, 0, true);
      //if (!CacheRbTreeVerify(&tx.db->pager.cacher.dirty_tree)) {
      //  printf("???");
      //}
      /*if (i == 4585) {
        printf("??");
      }
      if (i > 4337 && !CacheHashListGet(&tx.db->pager.cacher.lru_list, &pgid, false)) {
        printf("??");
      }
      */

      int n = 0;


      if (!BucketPut(&tx.meta_info.bucket, (void*)&iter, 8, (void*)&iter, 8)) {
        printf("NOW!");
      }
      //PrintBucket(&tx, BucketEntryToBPlusEntry((BucketEntry*)PagerReference(&db->pager, tx.meta_info.bucket.bp_tree.root_id)), 0, 0);
      //printf("\n");

      //BucketEntry* entry = (BucketEntry*)PagerReference(&tx.db->pager, tx.meta_info.bucket.bp_tree.root_id);
      //PrintBucket(&tx, BucketEntryToBPlusEntry(entry), 0, 0);
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
    //  TxBegin(db, &tx, kTxReadWrite);
    //}
    //i = 0;
    //for (auto& iter : map) {
    //  i++;
    //  
    //  if (m == 0) {
    //    TxBegin(db, &tx, kTxReadWrite);
    //    //printf("%d  ", GetBucketCount(&tx));
    //  }

    //
    //  if (!BucketPut(&tx.meta_info.bucket, (void*)&iter.first, 4, (void*)&iter.second, 4)) {
    //    printf("NOW!");
    //  }

    //  
    //  if (m == 0) {
    //    TxCommit(&tx);
    //  }
    //}
    //if (m == 1) {
    //  TxCommit(&tx);
    //}


    l = GetTickCount64() - l;
    if (l == 0) {
      l = 1;
    }
    printf("write: %dms, %dtps, %dns/op\n", (int)l, (int)(aaa.size() * 1000 / l), (int)((l * 1000 * 1000) / aaa.size()));
  }

  l = GetTickCount64();

  //BucketEntry* entry = (BucketEntry*)PagerReference(&tx.db->pager, 12);
  //printf("alloc_size %d, %p\n", entry->info.alloc_size, entry);
  //PrintBucket(&tx, BucketEntryToBPlusEntry(entry), 0, 0);
  //PagerDereference(&tx.db->pager, entry);


  if (m == 1) {
    TxBegin(db, &tx, kTxReadOnly);
  }

  //PrintBucket(&tx, BucketEntryToBPlusEntry((BucketEntry*)PagerReference(&db->pager, tx.meta_info.bucket.bp_tree.root_id)), 0, 0);
  //printf("%d", GetBucketCount(&tx));
  i = 0;
  for (auto& iter : aaa) {
    ++i;
    if (m == 0) {
      TxBegin(db, &tx, kTxReadOnly);
    }
    //int64_t kk = 6141830907859451818;
    //if ( !BucketFind(&tx.meta_info.bucket, (void*)&kk, 4)) {
    //  printf("??");
    //}

    //  //PrintBucket(&tx, BucketEntryToBPlusEntry((BucketEntry*)PagerReference(&db->pager, tx.meta_info.bucket.bp_tree.root_id)), 0, 0);

    //}
    if (!BucketFind(&tx.meta_info.bucket, (void*)&iter, 8)) {
      printf("NOR! %llx  %llx   ;", iter, iter);
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
  printf("read: %dms, %dtps, %dns/op", (int)l, (int)(aaa.size() * 1000 / l), (int)((l * 1000 * 1000) / aaa.size()));
  // YuDbPut(db, );
  // YuDbPut(db, "dwadad", 6, "123456", 6);

  printf("\n");


  YuDbClose(db);

  system("pause");
}



