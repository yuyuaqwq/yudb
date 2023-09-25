#ifndef YUDB_PAGE_H_
#define YUDB_PAGE_H_

#include <stdbool.h>
#include <stdint.h>



//#include <libyuc/container/rb_tree.h>

#ifdef    __cplusplus
extern "C" {
#endif //    __cplusplus

typedef int32_t PageId;
typedef int16_t PageOffset;
typedef int32_t PageCount;

extern const PageId kPageInvalidId;
extern const PageOffset kPageInvalidOffset;

#define LIBYUC_CONTAINER_VECTOR_CLASS_NAME PageId
#define LIBYUC_CONTAINER_VECTOR_INDEXER_Type_Element PageId
#define LIBYUC_CONTAINER_VECTOR_INDEXER_Type_Id PageId
#define LIBYUC_CONTAINER_VECTOR_INDEXER_Type_Offset PageCount
#include <libyuc/container/vector.h>

#define LIBYUC_CONTAINER_HASH_TABLE_CLASS_NAME PageId
#define LIBYUC_CONTAINER_HASH_TABLE_INDEXER_Type_Element PageId
#define LIBYUC_CONTAINER_HASH_TABLE_INDEXER_Type_Id PageId
#define LIBYUC_CONTAINER_HASH_TABLE_INDEXER_Type_Offset PageCount
#include <libyuc/container/hash_table.h>

#ifdef __cplusplus
}
#endif //    __cplusplus

#endif // YUDB_PAGE_H_
