#include "tx_manager.h"

#include "db_impl.h"


namespace yudb {

TxManager::TxManager(DBImpl* db) :
    db_{ db } {}

TxManager::~TxManager() {
    if (!view_tx_map_.empty()) {
        throw std::runtime_error("There are read transactions that have not been exited.");
    }
    if (update_tx_.has_value()) {
        throw std::runtime_error("There are write transactions that have not been exited.");
    }
}

UpdateTx TxManager::Update() {
    AppendBeginLog();
    update_tx_.emplace(this, db_->meta().meta_format(), true);
    update_tx_->set_txid(update_tx_->txid() + 1);
    if (update_tx_->txid() == kInvalidTxId) {
        throw std::runtime_error("txid overflow.");
    }
    const auto iter = view_tx_map_.cbegin();
    if (iter != view_tx_map_.end()) {
        pager().ClearPending(iter->first);
    } else {
        pager().ClearPending(kInvalidTxId);
    }
    return UpdateTx{ &*update_tx_ };
}

ViewTx TxManager::View() {
    const auto iter = view_tx_map_.find(db_->meta().meta_format().txid);
    if (iter == view_tx_map_.end()) {
        view_tx_map_.insert({ db_->meta().meta_format().txid , 1});
    } else {
        ++iter->second;
    }
    return ViewTx{ this, db_->meta().meta_format()};
}

void TxManager::RollBack() {
    AppendRollbackLog();
    pager().RollbackPending();
    update_tx_ = std::nullopt;
}

void TxManager::RollBack(TxId view_txid) {
    const auto iter = view_tx_map_.find(view_txid);
    assert(iter != view_tx_map_.end());
    assert(iter->second > 0);
    --iter->second;
    if (iter->second == 0) {
        view_tx_map_.erase(iter);
    }
}

void TxManager::Commit() {
    AppendCommitLog();
    pager().CommitPending();
    MetaFormatCopy(&db_->meta().meta_format(), update_tx_->meta_format());

    //pager().SyncAllPage();
    // 不应该在这里保存元数据，也不应该交换元页面
    //db_->meta().Save();
    //db_->meta().Switch();

    update_tx_ = std::nullopt;
}

TxImpl& TxManager::CurrentUpdateTx() {
    assert(update_tx_.has_value());
    return *update_tx_;
}

void TxManager::AppendPutLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    TxLogBucketFormat format;
    format.type = TxLogType::kBucketPut;
    format.bucket_id = bucket_id;
    std::array<std::span<const uint8_t>, 3> arr;
    arr[0] = { reinterpret_cast<const uint8_t*>(&format), sizeof(format) };
    arr[1] = key;
    arr[2] = value;
    AppendLog(arr.begin(), arr.end());
}

void TxManager::AppendInsertLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    TxLogBucketFormat format;
    format.type = TxLogType::kBucketInsert;
    format.bucket_id = bucket_id;
    std::array<std::span<const uint8_t>, 3> arr;
    arr[0] = { reinterpret_cast<const uint8_t*>(&format), sizeof(format) };
    arr[1] = key;
    arr[2] = value;
    AppendLog(arr.begin(), arr.end());
}

void TxManager::AppendDeleteLog(BucketId bucket_id, std::span<const uint8_t> key) {
    TxLogBucketFormat format;
    format.type = TxLogType::kBucketDelete;
    format.bucket_id = bucket_id;
    std::array<std::span<const uint8_t>, 2> arr;
    arr[0] = { reinterpret_cast<const uint8_t*>(&format), sizeof(format) };
    arr[1] = key;
    AppendLog(arr.begin(), arr.end());
}


Pager& TxManager::pager() const {
    return db_->pager();
}


template<typename Iter>
void TxManager::AppendLog(const Iter begin, const Iter end) {
    for (auto it = begin; it != end; ++it) {
        db_->log_writer().AppendRecordToBuffer(*it);
    }
}

void TxManager::AppendBeginLog() {
    TxLogType type = TxLogType::kBegin;
    std::array<std::span<const uint8_t>, 1> arr;
    arr[0] = { reinterpret_cast<const uint8_t*>(&type), sizeof(type) };
    AppendLog(arr.begin(), arr.end());
}

void TxManager::AppendRollbackLog() {
    TxLogType type = TxLogType::kRollback;
    std::array<std::span<const uint8_t>, 1> arr;
    arr[0] = { reinterpret_cast<const uint8_t*>(&type), sizeof(type) };
    AppendLog(arr.begin(), arr.end());
}

void TxManager::AppendCommitLog() {
    TxLogType type = TxLogType::kCommit;
    std::array<std::span<const uint8_t>, 1> arr;
    arr[0] = { reinterpret_cast<const uint8_t*>(&type), sizeof(type) };
    AppendLog(arr.begin(), arr.end());
    db_->log_writer().FlushBuffer();
}


} // namespace yudb