#ifndef YUDB_PAGE_H_
#define YUDB_PAGE_H_

#include <stdbool.h>
#include <stdint.h>

#include <libyuc/container/vector.h>
#include <libyuc/container/hash_table.h>
#include <libyuc/container/rb_tree.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

typedef int32_t PageId;
typedef int16_t PageOffset;
typedef int32_t PageCount;

extern const PageId kPageInvalidId;
extern const PageOffset kPageInvalidOffset;

LIBYUC_CONTAINER_VECTOR_DECLARATION(PageId, PageId)
LIBYUC_CONTAINER_HASH_TABLE_DECLARATION(PageId, PageId, PageId)

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_PAGE_H_
