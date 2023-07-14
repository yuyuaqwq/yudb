#include <yudb/transaction.h>

#include <yudb/yudb.h>
#include <yudb/wal.h>

#define LIBYUC_CONTINUE_RB_TREE_ACCESSOR_DEFALUT_GetKey(TREE, ENTRY) (&ObjectGetFromField(ENTRY, TxWriteRecordEntry, rb_entry)->txid)
#define YUDB_TX_RB_TREE_ACCESSOR LIBYUC_CONTINUE_RB_TREE_ACCESSOR_DEFALUT
LIBYUC_CONTAINER_RB_TREE_DEFINE(Tx, TxRbEntry*, TxId, LIBYUC_OBJECT_REFERENCER_DEFALUT, LIBYUC_CONTINUE_RB_TREE_ACCESSOR_DEFALUT, LIBYUC_OBJECT_COMPARER_DEFALUT)

const TxId kTxInvalidId = -1;

void TxFreePendingPoolPage(YuDb* db) {
    TxRbEntry* read_rb_entry = TxRbTreeIteratorFirst(&db->tx_manager.read_tx_record);
    TxReadRecordEntry* read_record_entry = ObjectGetFromField(read_rb_entry, TxReadRecordEntry, rb_entry);

    TxRbEntry* write_rb_entry = TxRbTreeIteratorFirst(&db->tx_manager.write_tx_record);
    // 遍历事务队列，找到比当前最小读事务id还小的写事务记录，将其pending页面释放
    while (write_rb_entry) {
        TxWriteRecordEntry* write_tx_record_entry = ObjectGetFromField(write_rb_entry, TxWriteRecordEntry, rb_entry);
        if (read_rb_entry == NULL || write_tx_record_entry->txid < read_record_entry->txid) {
            for (int i = 0; i < write_tx_record_entry->pending_pgid_arr.count; i++) {
                PagerFree(&db->pager, write_tx_record_entry->pending_pgid_arr.obj_arr[i], false);
            }
            write_rb_entry = TxRbTreeIteratorNext(&db->tx_manager.write_tx_record, write_rb_entry);
            TxRbTreeDelete(&db->tx_manager.write_tx_record, &write_tx_record_entry->rb_entry);
            PageIdVectorRelease(&write_tx_record_entry->pending_pgid_arr);
            ObjectRelease(write_tx_record_entry);
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
    // 更新读事务记录
    TxRbEntry* entry = TxRbTreeIteratorFirst(&tx->db->tx_manager.read_tx_record);
    TxReadRecordEntry* record_entry = ObjectGetFromField(entry, TxReadRecordEntry, rb_entry);
    if (entry == NULL || record_entry->txid != tx->meta_info.txid) {
        TxReadRecordEntry* read_record = ObjectCreate(TxReadRecordEntry);
        read_record->count = 1;
        read_record->txid = tx->meta_info.txid;
        TxRbTreePut(&tx->db->tx_manager.read_tx_record, &read_record->rb_entry);
    }
    else {
        record_entry->count++;
    }
}

static void TxBeginReadWrite(Tx* tx) {
    memcpy(&tx->meta_info, &tx->db->meta_info, sizeof(tx->meta_info));
    tx->meta_info.txid++;
    tx->meta_index = (tx->db->meta_index + 1) % 2;        // 不能覆写最后持久化版本的meta，永远写到可覆盖的meta

    // 将低于最低读事务id的写事务待决页面释放
    if (tx->db->tx_manager.write_tx_record.root == NULL) {
        // 初次开启写事务时，清理空闲表内所有pending页面
        FreeManagerCleanPending(&tx->db->pager.free_manager);
    } else {
        TxFreePendingPoolPage(tx->db);
    }
    
    TxWriteRecordEntry* pending_list_entry = ObjectCreate(TxWriteRecordEntry);
    pending_list_entry->txid = tx->meta_info.txid;
    PageIdVectorInit(&pending_list_entry->pending_pgid_arr, 4, true);
    pending_list_entry->pending_pgid_arr.count = 0;
    TxRbTreePut(&tx->db->tx_manager.write_tx_record, &pending_list_entry->rb_entry);

    if (tx->db->config.update_mode == kConfigUpdateWal) {
        WalAppendBeginLog(&tx->db->wal_manager);
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
        TxRbEntry* rb_entry = TxRbTreeFind(&tx->db->tx_manager.read_tx_record, &tx->meta_info.txid);
          assert(rb_entry);
        TxReadRecordEntry* read_record_entry = ObjectGetFromField(rb_entry, TxReadRecordEntry, rb_entry);
        assert(read_record_entry->count > 0);
        if (--read_record_entry->count == 0) {
            TxRbTreeDelete(&tx->db->tx_manager.read_tx_record, rb_entry);
            ObjectRelease(read_record_entry);
        }
        return;
    }
    memcpy(&tx->db->meta_info, &tx->meta_info, sizeof(tx->meta_info));
    if (tx->db->config.update_mode == kConfigUpdateInPlace) {
        PagerSyncWriteAllDirty(&tx->db->pager);
        FreeManagerWrite(&tx->db->pager.free_manager, tx->meta_index);
        MetaInfoWrite(tx->db, tx->meta_index);
        tx->db->meta_index = tx->meta_index;        // Wal模式不在提交时更新，因为元信息并未落盘，不能变动最近完成持久化版本
    }  
    else if (tx->db->config.update_mode == kConfigUpdateWal) {
        WalAppendCommitLog(&tx->db->wal_manager);
        if (++tx->db->pager.cacher.write_later_tx_count >= tx->db->config.wal_max_tx_count) {
            // 触发队列封存，切换新队列
            CacherWriteLaterQueueImmutable(&tx->db->pager.cacher);
        }
    }
}


void TxManagerInit(TxManager* tx_manager) {
    YuDb* db = ObjectGetFromField(tx_manager, YuDb, tx_manager);

    TxRbTreeInit(&tx_manager->write_tx_record);
    TxRbTreeInit(&tx_manager->read_tx_record);

    tx_manager->last_persistent_txid = db->meta_info.txid;
}
