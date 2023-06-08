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
typedef enum {
    kDataInline = 0,
    kDataBlock = 1,
    kDataPage = 2,
    kDataMemory = 3,
} DataType;
typedef struct _Data {
    union {
        uint8_t type : 2;
        struct {
            uint8_t type : 2;       // 00
            uint8_t invalid : 2;
            uint8_t size : 4;
            uint8_t data[11];        // 最大嵌入11字节的数据
        } inline_;        // 嵌入到element
        struct {
            uint16_t type : 2;      // 01
            uint16_t invalid : 14;
            uint16_t size;
            uint16_t element_id;
            uint64_t reserve;
        } block;        // entry中独立的块
        struct {
            uint64_t type : 2;      // 10
            uint64_t size : 62;     // 最大2^62-1
            PageId pgid;
        } page;     // 独立页面
        struct {
            uint32_t type : 2;     // 11
            uint32_t size : 30;    // 最大1G
            uintptr_t mem_ptr;     // addr，8字节
        } memory;       // 内存数据
    };
} Data;
#pragma pack()



#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_DATA_POOL_H_
