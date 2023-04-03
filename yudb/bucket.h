#ifndef YUDB_BUCKET_H_
#define YUDB_BUCKET_H_

#include <stdio.h>
#include <stdint.h>

#include <CUtils/container/bplus_tree.h>

#include <yudb/page.h>
#include <yudb/txid.h>
#include <yudb/data_pool.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus
    
CUTILS_CONTAINER_BPLUS_TREE_DECLARATION(YuDb, PageId, int32_t, int32_t)

typedef struct _BucketEntry {
    PageId first_data_pool[kDataPoolCount];
    TxId last_write_tx_id;
    YuDbBPlusEntry bp_entry;
} BucketEntry;

typedef struct _Bucket {
    YuDbBPlusTree bp_tree;
} Bucket;

void BucketInit(struct _YuDb* db, Bucket* tx);
bool BucketPut(Bucket* tx, void* key_buf, int16_t key_size, void* value_buf, size_t value_size);
bool BucketFind(Bucket* tx, void* key_buf, int16_t key_size);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_BUCKET_H_