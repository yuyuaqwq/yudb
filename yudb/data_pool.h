#ifndef YUDB_DATA_POOL_H_
#define YUDB_DATA_POOL_H_

#include <stdbool.h>
#include <stdint.h>

#include <CUtils/container/static_list.h>

#include <yudb/page.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

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
            uint16_t type : 2;      // 01
            uint16_t offset : 14;       // 4字节对齐，存储时右移2位，使用时左移2位
            PageSize size;
            PageId pgid;
        } block;        // 数据池中的块
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
#pragma pack()

extern const PageSize kDataPoolElementSize[];
#define kDataPoolCount 7

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_DATA_POOL_H_
