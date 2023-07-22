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
  Config config;
  int32_t meta_index;    // 最后持久化版本的meta索引
  MetaInfo meta_info;
  Pager pager;    // 页面管理器
  TxManager tx_manager;    // 事务管理器
  WalManager wal_manager;

  char err_msg[256];
} YuDb;

YuDb* YuDbOpen(const char* path, const Config* config);
void YuDbClose(YuDb* db);

const char* YuDbGetErrMsg(YuDb* db);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_YUDB_H_