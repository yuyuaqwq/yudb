#ifndef YUDB_DATA_POOL_H_
#define YUDB_DATA_POOL_H_

#include <stdbool.h>
#include <stdint.h>

#include <libyuc/space_manager/object_pool.h>
#include <libyuc/space_manager/free_list.h>

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
  uintptr_t mem_ptr;   // addr，8字节
} MemoryData;

typedef struct _DataDescriptor {
  union {
    uint8_t type : 2;
    struct {
      uint8_t type : 2;     // 00
      uint8_t invalid : 2;
      uint8_t size : 4;
      uint8_t data[3];    // 最大嵌入3字节的数据
    } inline_;    // 嵌入到element
    struct {
      uint16_t type : 2;    // 01
      uint16_t size : 14;   // 4字节对齐，>> 2 存储， << 2 使用
      uint16_t data_id;  // 指向页内实际数据
    } block;    // entry中独立的块
    //struct {
    //  uint64_t type : 2;    // 10
    //  uint16_t element_id;    // 指向页内的页描述块(描述长度、页信息(Page:4byte、logn:1byte))
    //} page;   // 独立页面
    struct {
      uint8_t type : 2;     // 11
      uint8_t is_value : 1;
    } mem_buf;
  };
} DataDescriptor;

LIBYUC_CONTAINER_SPACE_MANAGER_FREE_LIST_DECLARATION(DataPool, int16_t, uint8_t, 1)

typedef struct _DataPool {
  DataPoolFreeList free_list;
} DataPool;
#pragma pack()

extern MemoryData g_key;
extern MemoryData g_value;

void DataPoolInit(DataPool* data_pool, size_t pool_size);
int16_t DataPoolAlloc(DataPool* data_pool, int16_t size);
void DataPoolRelease(DataPool* data_pool, int16_t data_id, int16_t size);
void* DataPoolGetBlock(DataPool* data_pool, int16_t data_id);

void* DataDescriptorParser(DataPool* data_pool, DataDescriptor* data, int16_t* size);
int16_t DataDescriptorGetExpandSize(DataPool* data_pool, DataDescriptor* data);
void DataDescriptorReleaseExpand(DataPool* data_pool, DataDescriptor* data);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_DATA_POOL_H_
