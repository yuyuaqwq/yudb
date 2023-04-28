#include "yudb/transaction.h"

#include "yudb/yudb.h"
#include "yudb/wal.h"

#define CUTILS_CONTINUE_RB_TREE_ACCESSOR_DEFALUT_GetKey(TREE, ENTRY) (&ObjectGetFromField(ENTRY, TxPendingListEntry, rb_entry)->txid)
#define YUDB_TX_RB_TREE_ACCESSOR CUTILS_CONTINUE_RB_TREE_ACCESSOR_DEFALUT
CUTILS_CONTAINER_RB_TREE_DEFINE(Tx, TxRbEntry*, TxId, CUTILS_OBJECT_REFERENCER_DEFALUT, CUTILS_CONTINUE_RB_TREE_ACCESSOR_DEFALUT, CUTILS_OBJECT_COMPARER_DEFALUT)

const TxId kTxInvalidId = -1;

void TxFreePendingPoolPage(YuDb* db) {
	TxRbEntry* entry = TxRbTreeIteratorFirst(&db->tx_manager.pending_page_list);
	// 遍历事务队列，找到比当前最小读事务id还小的事务，将其pending页面释放
	while (entry) {
		TxPendingListEntry* pending_list_entry = ObjectGetFromField(entry, TxPendingListEntry, rb_entry);
		if (db->tx_manager.min_read_txid == kTxInvalidId || pending_list_entry->txid < db->tx_manager.min_read_txid) {
			for (int i = 0; i < pending_list_entry->pending_pgid_arr.count; i++) {
				PagerFree(&db->pager, pending_list_entry->pending_pgid_arr.obj_arr[i], false);
			}
			entry = TxRbTreeIteratorNext(&db->tx_manager.pending_page_list, entry);
			TxRbTreeDelete(&db->tx_manager.pending_page_list, &pending_list_entry->rb_entry);
			PageIdVectorRelease(&pending_list_entry->pending_pgid_arr);
			ObjectRelease(pending_list_entry);
		}
		else {
			//TxRbTreeDelete(&db->tx_manager.pending_page_list, &pending_list_entry->rb_entry);
			//ObjectRelease(pending_list_entry);
			break;
		}
	}
}

static void TxBeginReadOnly(Tx* tx) {
	// 拷贝元信息，是读事务拷贝时也需要加锁，避免写事务提交时修改元信息
	memcpy(&tx->meta_info, &tx->db->meta_info, sizeof(tx->meta_info));
	// 更新最小读事务id
	if (tx->db->tx_manager.min_read_txid == kTxInvalidId || tx->db->tx_manager.min_read_txid > tx->meta_info.txid) {
		tx->db->tx_manager.min_read_txid = tx->meta_info.txid;
	}
}

static void TxBeginReadWrite(Tx* tx) {
	memcpy(&tx->meta_info, &tx->db->meta_info, sizeof(tx->meta_info));
	tx->meta_info.txid++;
	tx->meta_index = (tx->db->meta_index + 1) % 2;		// 不能覆写最后已持久化的meta，永远写到可覆盖的meta

	// 将低于最低读事务id的写事务待决页面释放
	if (tx->db->tx_manager.pending_page_list.root == NULL) {
		// 初次开启写事务时，清理空闲表内所有pending页面
		FreeTableCleanPending(&tx->db->pager.free_table);
	} else {
		TxFreePendingPoolPage(tx->db);
	}
	
	TxPendingListEntry* pending_list_entry = ObjectCreate(TxPendingListEntry);
	pending_list_entry->txid = tx->meta_info.txid;
	PageIdVectorInit(&pending_list_entry->pending_pgid_arr, 4, true);
	pending_list_entry->pending_pgid_arr.count = 0;
	TxRbTreePut(&tx->db->tx_manager.pending_page_list, &pending_list_entry->rb_entry);

	if (tx->db->config->update_mode == kConfigUpdateWal) {
		WalAppendBeginLog(&tx->db->wal);
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
	if (tx->meta_info.bucket.bp_tree.root_id == 0) {
		// 未初始化的Bucket
		BucketInit(db, &tx->meta_info.bucket);
	}
}

void TxRollback(Tx* tx) {

}

void TxCommit(Tx* tx) {
	if (tx->type == kTxReadOnly) {
		if (tx->meta_info.txid == tx->db->tx_manager.min_read_txid) {
			// 更新最小读事务id
			TxRbEntry* entry = TxRbTreeIteratorFirst(&tx->db->tx_manager.pending_page_list);
			entry = TxRbTreeIteratorNext(&tx->db->tx_manager.pending_page_list, entry);
			if (entry) {
				TxPendingListEntry* pending_list_entry = ObjectGetFromField(entry, TxPendingListEntry, rb_entry);
				tx->db->tx_manager.min_read_txid = pending_list_entry->txid;
			}
			else {
				tx->db->tx_manager.min_read_txid = kTxInvalidId;
			}
		}
		return;
	}
	memcpy(&tx->db->meta_info, &tx->meta_info, sizeof(tx->meta_info));
	if (tx->db->config->update_mode == kConfigUpdateInPlace) {
		PagerSyncWriteAllDirty(&tx->db->pager);
		FreeTableWrite(&tx->db->pager.free_table, tx->meta_index);
		MetaInfoWrite(tx->db, tx->meta_index);
		tx->db->meta_index = tx->meta_index;		// Wal模式不在提交时更新，因为元信息并未落盘，不能变动最近完成持久化版本
	}  
	else if (tx->db->config->update_mode == kConfigUpdateWal) {
		WalAppendCommitLog(&tx->db->wal);
	}
}


void TxManagerInit(TxManager* tx_manager) {
	YuDb* db = ObjectGetFromField(tx_manager, YuDb, tx_manager);

	TxRbTreeInit(&tx_manager->pending_page_list);

	tx_manager->min_read_txid = kTxInvalidId;
	tx_manager->last_persistent_txid = db->meta_info.txid;

	// 基于持久化的版本重放日志，重放日志时不会再次写日志
}
