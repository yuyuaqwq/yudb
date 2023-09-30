#include <yudb/page.h>

const PageId kPageInvalidId = -1;
const PageOffset kPageInvalidOffset = -1;


#define LIBYUC_CONTAINER_VECTOR_CLASS_NAME PageId
#define LIBYUC_CONTAINER_VECTOR_INDEXER_Type_Element PageId
#define LIBYUC_CONTAINER_VECTOR_INDEXER_Type_Id PageId
#define LIBYUC_CONTAINER_VECTOR_INDEXER_Type_Offset PageCount
#include <libyuc/container/vector.c>

#define LIBYUC_CONTAINER_HASH_TABLE_CLASS_NAME PageId
#define LIBYUC_CONTAINER_HASH_TABLE_INDEXER_Type_Element PageId
#define LIBYUC_CONTAINER_HASH_TABLE_INDEXER_Type_Id PageId
#define LIBYUC_CONTAINER_HASH_TABLE_INDEXER_Type_Offset PageCount
#include <libyuc/container/hash_table.c>