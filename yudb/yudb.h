#ifndef YUDB_YUDB_H_
#define YUDB_YUDB_H_

#include <stdio.h>
#include <stdint.h>

#include <yudb/db_file.h>
#include <yudb/pager.h>
#include <yudb/transaction.h>
#include <yudb/bucket.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

#define YUDB_VERSION 1

#define YUDB_MODE_SYNC_OFF 0
#define YUDB_MODE_SYNC_NORMAL 1
#define YUDB_MODE_SYNC_FULL 2
#define YUDB_MODE_SYNC YUDB_MODE_SYNC_NORMAL

typedef enum _YuDbSyncMode {
	kYuDbSyncOff = 0,
	kYuDbSyncNormal = 1,
	kYuDbSyncFull = 2,
} YuDbSyncMode;

typedef enum _YuDbUpdateMode {
	kYuDbUpdateInPlace = 0,
	kYuDbUpdateWal = 1,
} YuDbUpdateMode;

typedef struct _YuDb {
	DbFile* db_file;
	DbFile* log_file;
	YuDbSyncMode sync_mode;
	YuDbUpdateMode update_mode;
	int32_t meta_index;		// 当前最新完成提交的事务的meta索引
	MetaInfo meta_info;
	Pager pager;		// 页面管理器
	TxManager tx_manager;		// 事务管理器
} YuDb;

YuDb* YuDbOpen(const char* path, YuDbSyncMode sync_mode);
void YuDbClose(YuDb* db);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_YUDB_H_