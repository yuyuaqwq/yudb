#ifndef YUDB_META_INFO_H_
#define YUDB_META_INFO_H_

#include <stdio.h>
#include <stdint.h>

#include <yudb/bucket.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

typedef struct {
	uint32_t magic;
	uint16_t page_size;
	PageId page_count;
	Bucket bucket;
	uint32_t txid;
	uint64_t check_sum;
} MetaInfo;

#ifdef  __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_META_INFO_H_