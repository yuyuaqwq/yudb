#ifndef YUDB_PAGE_H_
#define YUDB_PAGE_H_

#include <stdbool.h>
#include <stdint.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

typedef int32_t PageId;
typedef int16_t PageSize;
typedef int16_t PageOffset;
typedef int32_t PageCount;

extern const PageId kPageInvalidId;

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_PAGE_H_
