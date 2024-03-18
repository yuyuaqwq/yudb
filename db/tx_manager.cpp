#include "yudb/tx_manager.h"

#include "yudb/db_impl.h"
#include "yudb/log_type.h"
#include "yudb/error.h"

namespace yudb {

TxManager::TxManager(DBImpl* db) :
    db_{ db }
{
    pager().LoadFreeList();
    min_view_txid_ = db_->meta().meta_struct().txid;
}

TxManager::~TxManager() {
    std::unique_lock lock{ db_->shm()->meta_lock() };
    if (update_tx_.has_value()) {
        throw TxManagerError{ "there are write transactions that have not been exited." };
    }
    if (!view_tx_map_.empty()) {
        throw TxManagerError{ "there are read transactions that have not been exited." };
    }
}

TxImpl& TxManager::Update(Comparator comparator) {
    db_->shm()->update_lock().lock();
    db_->ClearMmap();

    assert(!update_tx_.has_value());
    AppendBeginLog();

    std::unique_lock lock{ db_->shm()->meta_lock() };
    update_tx_.emplace(this, db_->meta().meta_struct(), true, comparator);
    update_tx_->set_txid(update_tx_->txid() + 1);
    if (update_tx_->txid() == kTxInvalidId) {
        throw TxManagerError("txid overflow.");
    }
    auto min_view_txid = min_view_txid_.load();
    pager().Release(min_view_txid - 1);
    return *update_tx_;
}

ViewTx TxManager::View(Comparator comparator) {
    std::unique_lock lock{ db_->shm()->meta_lock() };
    auto txid = db_->meta().meta_struct().txid;
    const auto iter = view_tx_map_.find(txid);
    if (iter == view_tx_map_.end()) {
        view_tx_map_.insert({ txid, 1});
    } else {
        ++iter->second;
    }
    return ViewTx{ this, db_->meta().meta_struct(), &db_->db_file_mmap_lock(), comparator };
}

void TxManager::RollBack() {
    std::unique_lock lock{ db_->shm()->meta_lock() };
    if (view_tx_map_.empty()) {
        min_view_txid_.store(update_tx_->txid() - 1);
    }

    AppendRollbackLog();
    if (db_->logger().CheckPointNeeded()) {
        db_->logger().Checkpoint();
    }
    pager().Rollback();
    update_tx_ = std::nullopt;
    db_->shm()->update_lock().unlock();
}

void TxManager::RollBack(TxId view_txid) {
    std::unique_lock lock{ db_->shm()->meta_lock() };
    const auto iter = view_tx_map_.find(view_txid);
    assert(iter != view_tx_map_.end());
    assert(iter->second > 0);
    --iter->second;
    if (iter->second == 0) {
        view_tx_map_.erase(iter);
        if (iter->first == min_view_txid_.load()) {
            if (view_tx_map_.empty()) {
                min_view_txid_.store(db_->meta().meta_struct().txid, std::memory_order_release);
            } else {
                const auto iter = view_tx_map_.cbegin();
                assert(iter != view_tx_map_.end());
                min_view_txid_.store(iter->first);
            }
        }
    }
}

void TxManager::Commit() {
    std::unique_lock lock{ db_->shm()->meta_lock() };
    if (view_tx_map_.empty()) {
        min_view_txid_.store(update_tx_->txid() - 1);
    }

    db_->meta().Reset(update_tx_->meta_struct());
    AppendCommitLog();
    if (db_->logger().CheckPointNeeded()) {
        db_->logger().Checkpoint();
    }
    update_tx_ = std::nullopt;
    db_->shm()->update_lock().unlock();
}

bool TxManager::IsTxExpired(TxId txid) const {
    return txid < min_view_txid_.load();
}

void TxManager::AppendSubBucketLog(BucketId bucket_id, std::span<const uint8_t> key) {
    BucketLogHeader format;
    format.type = LogType::kSubBucket;
    format.bucket_id = bucket_id;
    std::span<const uint8_t> arr[2];
    arr[0] = { reinterpret_cast<const uint8_t*>(&format), kBucketSubBucketLogHeaderSize };
    arr[1] = key;
    db_->logger().AppendLog(std::begin(arr), std::end(arr));
}

void TxManager::AppendPutLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket) {
    BucketLogHeader format;
    if (is_bucket) {
        format.type = LogType::kPut_IsBucket;
    } else {
        format.type = LogType::kPut_NotBucket;
    }
    format.bucket_id = bucket_id;
    std::span<const uint8_t> arr[3];
    arr[0] = { reinterpret_cast<const uint8_t*>(&format), kBucketPutLogHeaderSize };
    arr[1] = key;
    arr[2] = value;
    db_->logger().AppendLog(std::begin(arr), std::end(arr));
}

void TxManager::AppendDeleteLog(BucketId bucket_id, std::span<const uint8_t> key) {
    BucketLogHeader format;
    format.type = LogType::kDelete;
    format.bucket_id = bucket_id;
    std::span<const uint8_t> arr[2];
    arr[0] = { reinterpret_cast<const uint8_t*>(&format), kBucketDeleteLogHeaderSize };
    arr[1] = key;
    db_->logger().AppendLog(std::begin(arr), std::end(arr));
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
    db_->logger().AppendLog(std::begin(arr), std::end(arr));
}

void TxManager::AppendRollbackLog() {
    LogType type = LogType::kRollback;
    std::span<const uint8_t> arr[1];
    arr[0] = { reinterpret_cast<const uint8_t*>(&type), sizeof(type) };
    db_->logger().AppendLog(std::begin(arr), std::end(arr));
}

void TxManager::AppendCommitLog() {
    LogType type = LogType::kCommit;
    std::span<const uint8_t> arr[1];
    arr[0] = { reinterpret_cast<const uint8_t*>(&type), sizeof(type) };
    db_->logger().AppendLog(std::begin(arr), std::end(arr));
    db_->logger().FlushLog();
}

} // namespace yudb