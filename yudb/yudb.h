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

typedef struct _YuDb {
	DbFile* db_file;
	MetaInfo meta_info;
	Pager pager;		// 页面管理器
	TxManager tx_manager;		// 事务管理器
} YuDb;

YuDb* YuDbOpen(const char* path);
void YuDbPut(YuDb* db, void* key, int16_t key_size, void* value, size_t value_size);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_YUDB_H_