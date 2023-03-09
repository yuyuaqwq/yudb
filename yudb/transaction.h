#ifndef YUDB_TRANSACTION_H_
#define YUDB_TRANSACTION_H_

#include <stdbool.h>
#include <stdint.h>

#include <CUtils/container/rb_tree.h>

#include <yudb/page.h>
#include <yudb/meta_info.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

typedef int32_t TxId;
extern const TxId kTxInvalidId;

typedef enum {
	kTxReadOnly,
	kTxReadWrite,
} TxType;

typedef struct _Tx {
	TxType type;
	struct _YuDb* db;
	MetaInfo meta_info;
} Tx;

typedef struct _TxPendingListEntry {
	TxId txid;
	PageId first_pgid;
	RbEntry rb_entry;
} TxPendingListEntry;

typedef struct _TxManager {
	RbTree pending_page_list;		// 不同事务释放的待决页面
	TxId min_read_txid;		// 当前正在进行的最小读事务id
} TxManager;

void TxManagerInit(TxManager* tx_manager);

void TxBegin(struct _YuDb* db, Tx* tx, TxType type);
void TxRollback(Tx* tx);
void TxCommit(Tx* tx);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_TRANSACTION_H_