#ifndef YUDB_BUCKET_H_
#define YUDB_BUCKET_H_

#include <stdio.h>
#include <stdint.h>

#include <yudb/page.h>
#include <yudb/txid.h>
#include <yudb/data_pool.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus
    
#define CUTILS_CONTAINER_BPLUS_TREE_MODE_DISK
typedef Data Key;
typedef Data Value;

#define CUTILS_CONTAINER_BPLUS_TREE_DEFINE_BPlusEntry \
    typedef struct _BPlusEntry { \
        BPlusEntryType type; \
        uint32_t element_count; \
        PageId first_data_pool[kDataPoolCount]; \
        TxId last_write_tx_id; \
        union { \
            BPlusIndexEntry index; \
            BPlusLeafEntry leaf; \
        }; \
    } BPlusEntry; \

#define CUTILS_CONTAINER_BPLUS_TREE_DEFINE_BPlusTree \
    typedef struct _BPlusTree { \
        PageId root_id; \
        PageId leaf_list_first; \
        uint32_t index_m; \
        uint32_t leaf_m; \
    } BPlusTree; \

#include <CUtils/container/bplus_tree.h>
typedef BPlusTree Bucket;



void BucketInit(struct _YuDb* db, struct _Tx* tx);
bool BucketInsert(struct _Tx* tx, void* key_buf, int16_t key_size, void* value_buf, size_t value_size);
bool BucketFind(struct _Tx* tx, void* key_buf, int16_t key_size);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_BUCKET_H_