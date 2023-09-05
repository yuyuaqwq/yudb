#ifndef YUDB_META_INFO_H_
#define YUDB_META_INFO_H_

#include <stdio.h>
#include <stdint.h>

#include <yudb/bucket.h>
#include <yudb/config.h>

#ifdef    __cplusplus
extern "C" {
#endif //    __cplusplus

#pragma pack(1)
typedef struct {
    uint32_t magic;
    uint32_t min_version;
    uint16_t page_size;
    PageId page_count;
    Bucket bucket;
    uint32_t txid;
    time_t time;        // 为了避免txid回卷，另外记录事务的提交时间；如果提交时检测到发生回卷，就会强制使用未来时间(cur_time+1)，以避免相同的时间记录
    uint32_t crc32;
} MetaInfo;
#pragma pack()

bool MetaInfoRead(struct _YuDb* db, Config* config);
bool MetaInfoWrite(struct _YuDb* db, int32_t meta_index);

#ifdef    __cplusplus
}
#endif //    __cplusplus

#endif // YUDB_META_INFO_H_