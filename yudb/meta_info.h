#ifndef YUDB_META_INFO_H_
#define YUDB_META_INFO_H_

#include <stdio.h>
#include <stdint.h>

//#include <yudb/bucket.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

typedef struct {
	uint32_t magic;
	uint32_t min_version;
	uint16_t page_size;
	PageId page_count;
	Bucket bucket;
	uint32_t txid;
} MetaInfo;

bool MetaInfoWrite(struct _YuDb* db, int32_t meta_index);

#ifdef  __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_META_INFO_H_