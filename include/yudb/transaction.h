#ifndef YUDB_TRANSACTION_H_
#define YUDB_TRANSACTION_H_

#include <stdbool.h>
#include <stdint.h>

#include <libyuc/container/rb_tree.h>
#include <libyuc/container/vector.h>

#include <yudb/page.h>
#include <yudb/txid.h>
#include <yudb/meta_info.h>

LIBYUC_CONTAINER_RB_TREE_DECLARATION(Tx, struct _TxRbEntry*, TxId)

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

extern const TxId kTxInvalidId;

typedef enum {
  kTxReadOnly,
  kTxReadWrite,
} TxType;

typedef struct _Tx {
  TxType type;
  struct _YuDb* db;
  int32_t meta_index;    // 当前若为写事务，提交时落盘的索引
  MetaInfo meta_info;
} Tx;

typedef struct _TxReadRecordEntry {
  TxId txid;
  TxRbEntry rb_entry;
  int32_t count;
} TxReadRecordEntry;

typedef struct _TxWriteRecordEntry {
  TxId txid;
  TxRbEntry rb_entry;
  PageIdVector pending_pgid_arr;
} TxWriteRecordEntry;

typedef struct _TxManager {
  TxRbTree write_tx_record;    // 写事务记录，包括不同事务释放的待决页面，TxId为key
  TxRbTree read_tx_record;    // 读事务记录，顺序链表，TxId为key
  TxId last_persistent_txid;    // 最后持久化事务id，wal模式使用
} TxManager;

void TxFreePendingPoolPage(struct _YuDb* db);

void TxBegin(struct _YuDb* db, Tx* tx, TxType type);
void TxRollback(Tx* tx);
void TxCommit(Tx* tx);

void TxManagerInit(TxManager* tx_manager);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_TRANSACTION_H_