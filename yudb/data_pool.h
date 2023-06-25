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

typedef struct _MemoryData {
    uint32_t size;
    uintptr_t mem_ptr;     // addr，8字节
} MemoryData;

typedef struct _Data {
    union {
        uint8_t type : 2;
        struct {
            uint8_t type : 2;       // 00
            uint8_t invalid : 2;
            uint8_t size : 4;
            uint8_t data[3];        // 最大嵌入3字节的数据
        } inline_;        // 嵌入到element
        struct {
            uint16_t type : 2;      // 01
            uint16_t size : 14;     // 4字节对齐，>> 2 存储， << 2 使用
            uint16_t element_id;    // 指向页内实际数据
        } block;        // entry中独立的块
        //struct {
        //    uint64_t type : 2;      // 10
        //    uint16_t element_id;      // 指向页内的页描述块(描述长度、页信息(Page:4byte、logn:1byte))
        //} page;     // 独立页面
        struct {
            uint8_t type : 2;       // 11
            uint8_t is_value : 1;
        } mem_buf;
    };
} Data;
#pragma pack()



#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_DATA_POOL_H_
