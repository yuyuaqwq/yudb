#ifndef YUDB_YUDB_H_
#define YUDB_YUDB_H_

#include <stdio.h>
#include <stdint.h>

#include <yudb/config.h>
#include <yudb/db_file.h>
#include <yudb/pager.h>
#include <yudb/transaction.h>
#include <yudb/bucket.h>
#include <yudb/wal.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

#define YUDB_VERSION 1



typedef struct _YuDb {
	DbFile* db_file;
	Config* config;
	int32_t meta_index;		// 当前最新完成提交的事务的meta索引
	MetaInfo meta_info;
	Pager pager;		// 页面管理器
	TxManager tx_manager;		// 事务管理器
	WalManager wal;
} YuDb;

YuDb* YuDbOpen(const char* path, Config* config);
void YuDbClose(YuDb* db);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_YUDB_H_