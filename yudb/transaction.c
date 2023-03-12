#include "yudb/transaction.h"

#include "yudb/yudb.h"
#include "yudb/wal.h"

const TxId kTxInvalidId = -1;

void TxManagerInit(TxManager* tx_manager) {
	YuDb* db = ObjectGetFromField(tx_manager, YuDb, tx_manager);

	RbTreeInitByField(&tx_manager->pending_page_list, TxPendingListEntry, rb_entry, txid);
	tx_manager->min_read_txid = kTxInvalidId;

	tx_manager->last_persistent_txid = db->meta_info.txid;

	// 基于持久化的版本重放日志，重放日志时不会再次写日志
}

static void TxBeginReadOnly(Tx* tx) {
	memcpy(&tx->meta_info, &tx->db->meta_info, sizeof(tx->meta_info));		// 拷贝元信息，是读事务拷贝时也需要加锁，避免写事务提交时修改元信息
	if (tx->db->tx_manager.min_read_txid == kTxInvalidId || tx->db->tx_manager.min_read_txid > tx->meta_info.txid) {
		tx->db->tx_manager.min_read_txid = tx->meta_info.txid;
	}
}

static void TxBeginReadWrite(Tx* tx) {
	memcpy(&tx->meta_info, &tx->db->meta_info, sizeof(tx->meta_info));
	tx->meta_info.txid++;
	tx->meta_index = (tx->db->meta_index + 1) % 2;		// 不直接覆写已持久化的meta


	// 将低于最低读事务id的写事务待决页面释放
	RbTreeIterator iter;
	TxPendingListEntry* pending_list_entry = RbTreeFirst(&tx->db->tx_manager.pending_page_list, &iter);
	if (!pending_list_entry) {
		// 初次开启写事务时，清理所有pending页面
		FreeTableCleanPending(&tx->db->pager.free_table);
	}
	while (pending_list_entry) {
		if (tx->db->tx_manager.min_read_txid == kTxInvalidId || pending_list_entry->txid < tx->db->tx_manager.min_read_txid) {
			if (pending_list_entry->first_pending_pgid != kPageInvalidId) {
				PagerFreePending(&tx->db->pager, pending_list_entry->first_pending_pgid);
			}
			TxPendingListEntry* next = RbTreeNext(&iter);
			RbTreeDeleteEntry(&tx->db->tx_manager.pending_page_list, &pending_list_entry->rb_entry);
			ObjectDelete(pending_list_entry);
			pending_list_entry = next;
		}
		else {
			RbTreeDeleteEntry(&tx->db->tx_manager.pending_page_list, &pending_list_entry->rb_entry);
			ObjectDelete(pending_list_entry);
			break;
		}
	}

	pending_list_entry = ObjectCreate(TxPendingListEntry);
	pending_list_entry->txid = tx->meta_info.txid;
	pending_list_entry->first_pending_pgid = kPageInvalidId;
	RbTreeInsertEntryByKey(&tx->db->tx_manager.pending_page_list, &pending_list_entry->rb_entry);

	if (tx->db->update_mode == kYuDbUpdateWal) {
		LogAppendBegin(tx->db->log_file);
	}
}

void TxBegin(YuDb* db, Tx* tx, TxType type) {
	tx->db = db;
	tx->type = type;
	if (type == kTxReadWrite) {
		TxBeginReadWrite(tx);
	}
	else {
		TxBeginReadOnly(tx);
	}
	if (tx->meta_info.bucket.root_id == 0) {
		// 未初始化的Bucket
		BucketInit(db, tx);
	}
}

void TxRollback(Tx* tx) {

}

void TxCommit(Tx* tx) {
	if (tx->type != kTxReadWrite) {
		return;
	}
	memcpy(&tx->db->meta_info, &tx->meta_info, sizeof(tx->meta_info));
	if (tx->db->update_mode == kYuDbUpdateInPlace) {
		PagerWriteAllDirty(&tx->db->pager);
		FreeTableWrite(&tx->db->pager.free_table, tx->meta_index);
		MetaInfoWrite(tx->db, tx->meta_index);
	}
	else if (tx->db->update_mode == kYuDbUpdateWal) {
		LogAppendCommit(tx->db->log_file);
	}
	if (tx->db->update_mode == kYuDbUpdateInPlace) {
		tx->db->meta_index = tx->meta_index;
	}  // Wal模式不在提交时更新，因为元信息并未落盘
}