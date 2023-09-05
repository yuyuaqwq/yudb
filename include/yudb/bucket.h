#ifndef YUDB_BUCKET_H_
#define YUDB_BUCKET_H_

#include <stdio.h>
#include <stdint.h>

#include <libyuc/container/bplus_tree.h>

#include <yudb/page.h>
#include <yudb/txid.h>
#include <yudb/data_pool.h>

#ifdef    __cplusplus
extern "C" {
#endif //    __cplusplus
    

typedef DataDescriptor YuDbKey;
typedef DataDescriptor YuDbValue;

LIBYUC_CONTAINER_BPLUS_TREE_DECLARATION(YuDb, LIBYUC_CONTAINER_BPLUS_TREE_LEAF_LINK_MODE_NOT_LINK, PageId, int16_t, YuDbKey, YuDbValue, 32)

#pragma pack(1)
typedef struct _BucketEntryInfo {
    TxId last_write_tx_id;
    int16_t alloc_size;
    int16_t page_size;
    DataPool data_pool;
} BucketEntryInfo;

typedef struct _BucketEntry {
    BucketEntryInfo info;
    // YuDbBPlusEntry bp_entry;
} BucketEntry;
#pragma pack()

typedef struct _Bucket {
    YuDbBPlusTree bp_tree;
} Bucket;

void BucketInit(struct _YuDb* db, Bucket* bucket);
bool BucketPut(Bucket* bucket, void* key_buf, int16_t key_size, void* value_buf, size_t value_size);
bool BucketFind(Bucket* bucket, void* key_buf, int16_t key_size);

#ifdef __cplusplus
}
#endif //    __cplusplus

#endif // YUDB_BUCKET_H_