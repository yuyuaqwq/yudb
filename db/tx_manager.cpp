#include "yudb/tx_manager.h"

#include "yudb/db_impl.h"
#include "yudb/log_type.h"
#include "yudb/error.h"

namespace yudb {

TxManager::TxManager(DBImpl* db) :
    db_{ db } {}

TxManager::~TxManager() {
    if (update_tx_.has_value()) {
        throw TxManagerError{ "there are write transactions that have not been exited." };
    }

    std::unique_lock lock{ db_->shm()->meta_lock() };
    if (!view_tx_map_.empty()) {
        throw TxManagerError{ "there are read transactions that have not been exited." };
    }
}

TxImpl& TxManager::Update() {
    db_->shm()->update_lock().lock();

    assert(!update_tx_.has_value());
    AppendBeginLog();

    std::unique_lock lock{ db_->shm()->meta_lock() };
    update_tx_.emplace(this, db_->meta().meta_struct(), true);
    update_tx_->set_txid(update_tx_->txid() + 1);
    if (update_tx_->txid() == kTxInvalidId) {
        throw TxManagerError("txid overflow.");
    }
    if (first_) {
        first_ = false;

        pager().LoadFreeList();
    }
    const auto iter = view_tx_map_.cbegin();
    TxId min_txid;
    if (iter != view_tx_map_.cend()) {
        min_txid = iter->first;
    } else {
        min_txid = update_tx_->txid();
    }
    pager().Release(min_txid - 1);

    return *update_tx_;
}

ViewTx TxManager::View() {
    std::unique_lock lock{ db_->shm()->meta_lock() };
    auto txid = db_->meta().meta_struct().txid;
    const auto iter = view_tx_map_.find(txid);
    if (iter == view_tx_map_.end()) {
        view_tx_map_.insert({ txid, 1});
    } else {
        ++iter->second;
    }
    return ViewTx{ this, db_->meta().meta_struct() };
}

void TxManager::RollBack() {
    AppendRollbackLog();
    pager().Rollback();
    update_tx_ = std::nullopt;
    db_->shm()->update_lock().unlock();
}

void TxManager::RollBack(TxId view_txid) {
    db_->db_file_mmap_lock().unlock_shared();

    std::unique_lock lock{ db_->shm()->meta_lock() };
    const auto iter = view_tx_map_.find(view_txid);
    assert(iter != view_tx_map_.end());
    assert(iter->second > 0);
    --iter->second;
    if (iter->second == 0) {
        view_tx_map_.erase(iter);
    }
}

void TxManager::Commit() {
    committing_ = true;

    db_->meta().Reset(update_tx_->meta_format());
    AppendCommitLog();

    update_tx_ = std::nullopt;
    committing_ = false;

    db_->shm()->update_lock().unlock();
    db_->ClearMmap();
}

void TxManager::AppendPutLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    BucketLogHeader format;
    format.type = LogType::kPut;
    format.bucket_id = bucket_id;
    std::span<const uint8_t> arr[3];
    arr[0] = { reinterpret_cast<const uint8_t*>(&format), kBucketPutLogHeaderSize };
    arr[1] = key;
    arr[2] = value;
    db_->logger()->AppendLog(std::begin(arr), std::end(arr));
}

void TxManager::AppendInsertLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    BucketLogHeader format;
    format.type = LogType::kInsert;
    format.bucket_id = bucket_id;
    std::span<const uint8_t> arr[3];
    arr[0] = { reinterpret_cast<const uint8_t*>(&format), kBucketInsertLogHeaderSize };
    arr[1] = key;
    arr[2] = value;
    db_->logger()->AppendLog(std::begin(arr), std::end(arr));
}

void TxManager::AppendDeleteLog(BucketId bucket_id, std::span<const uint8_t> key) {
    BucketLogHeader format;
    format.type = LogType::kDelete;
    format.bucket_id = bucket_id;
    std::span<const uint8_t> arr[2];
    arr[0] = { reinterpret_cast<const uint8_t*>(&format), kBucketDeleteLogHeaderSize };
    arr[1] = key;
    db_->logger()->AppendLog(std::begin(arr), std::end(arr));
}

DBImpl& TxManager::db() {
    return *db_;
}

TxImpl& TxManager::update_tx() {
    assert(update_tx_.has_value());
    return *update_tx_;
}

Pager& TxManager::pager() const {
    return db_->pager();
}

void TxManager::AppendBeginLog() {
    LogType type = LogType::kBegin;
    std::span<const uint8_t> arr[1];
    arr[0] = { reinterpret_cast<const uint8_t*>(&type), sizeof(type) };
    db_->logger()->AppendLog(std::begin(arr), std::end(arr));
}

void TxManager::AppendRollbackLog() {
    LogType type = LogType::kRollback;
    std::span<const uint8_t> arr[1];
    arr[0] = { reinterpret_cast<const uint8_t*>(&type), sizeof(type) };
    db_->logger()->AppendLog(std::begin(arr), std::end(arr));
}

void TxManager::AppendCommitLog() {
    LogType type = LogType::kCommit;
    std::span<const uint8_t> arr[1];
    arr[0] = { reinterpret_cast<const uint8_t*>(&type), sizeof(type) };
    db_->logger()->AppendLog(std::begin(arr), std::end(arr));
    db_->logger()->FlushLog();
}

} // namespace yudb