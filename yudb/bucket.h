#ifndef YUDB_BUCKET_H_
#define YUDB_BUCKET_H_

#include <stdio.h>
#include <stdint.h>

#include <yudb/page.h>
#include <yudb/txid.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus
    
#define CUTILS_CONTAINER_BPLUS_TREE_MODE_DISK
#pragma pack(1)

typedef struct _MemoryData {
    void* buf;
    size_t size;
} MemoryData;
typedef enum {
    kDataEmbed = 0,
    kDataBlock = 1,
    kDataEach = 2,
    kDataMemory = 3,
} DataType;
typedef struct _Data {
    union {
        struct {
            uint8_t type : 2;       // 00
            uint8_t invalid : 3;
            uint8_t size : 3;
            uint8_t data[7];        // 最大嵌入7字节的数据
        } embed;        // 嵌入
        struct {
            uint32_t type : 2;      // 01
            uint32_t offset : 14;       // 4字节对齐，存储时右移2位，使用时左移2位
            uint32_t size : 16;
            PageId pgid;
        } block;        // 页面块
        struct {
            uint32_t type : 2;      // 10
            uint32_t size : 30;     // 暂时只支持1G的数据，如果需要扩展可以使这里为0，具体大小存储到第一个独立页面头部的8字节
            PageId pgid;
        } each;     // 独立页面
        struct {
            uintptr_t type : 2;     // 11
            uintptr_t mem_data : sizeof(uintptr_t) * 8 - 2;       // 4字节对齐，左移2位
        } memory;       // 内存数据
    };
} Data;
typedef Data Key;
typedef Data Value;
#pragma pack()

#define CUTILS_CONTAINER_BPLUS_TREE_DEFINE_BPlusEntry \
    typedef struct _BPlusEntry { \
        BPlusEntryType type; \
        uint32_t element_count; \
        PageId overflow_head_pgid; \
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