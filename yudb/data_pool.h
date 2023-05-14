#ifndef YUDB_DATA_POOL_H_
#define YUDB_DATA_POOL_H_

#include <stdbool.h>
#include <stdint.h>

#include <CUtils/space_manager/object_pool.h>

#include <yudb/page.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

#pragma pack(1)
typedef struct _MemoryBuffer {
    void* buf;
    size_t size;
} MemoryBuffer;

typedef enum {
    kDataInline = 0,
    kDataBlock = 1,
    kDataPage = 2,
    kDataMemory = 3,
} DataType;
typedef struct _Data {
    union {
        struct {
            uint8_t type : 2;       // 00
            uint8_t invalid : 3;
            uint8_t size : 3;
            uint8_t data[7];        // 最大嵌入7字节的数据
        } inline_;        // 嵌入
        struct {
            uint16_t type : 2;      // 01
            uint16_t offset : 14;       // 4字节对齐，存储时右移2位，使用时左移2位
            PageSize size;
            PageId pgid;
        } block;        // 数据池中的块
        struct {
            uint32_t type : 2;      // 10
            uint32_t size_type : 1; // 0表示size存储在29bit，否则存储到第一个独立页面头部的8字节
            uint32_t size : 29;     // 最大512M
            PageId pgid;
        } page;     // 独立页面
        struct {
            uintptr_t type : 2;     // 11
            uintptr_t mem_data : sizeof(uintptr_t) * 8 - 2;       // 4字节对齐，左移2位
        } memory;       // 内存数据
    };
} Data;
#pragma pack()



#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_DATA_POOL_H_
