#include "yudb/tx_manager.h"

#include "yudb/db_impl.h"
#include "yudb/operator_log_format.h"
#include "yudb/error.h"

namespace yudb {

TxManager::TxManager(DBImpl* db) :
    db_{ db } {}

TxManager::~TxManager() {
    if (!view_tx_map_.empty()) {
        throw TxManagerError{ "there are read transactions that have not been exited." };
    }
    if (update_tx_.has_value()) {
        throw TxManagerError{ "there are write transactions that have not been exited." };
    }
}

UpdateTx TxManager::Update() {
    assert(!update_tx_.has_value());
    AppendBeginLog();
    update_tx_.emplace(this, db_->meta().meta_format(), true);
    update_tx_->set_txid(update_tx_->txid() + 1);
    if (update_tx_->txid() == kTxInvalidId) {
        throw TxManagerError("txid overflow.");
    }
    if (first_) {
        first_ = false;
        pager().ClearPending();
    }
    const auto iter = view_tx_map_.cbegin();
    if (iter != view_tx_map_.end()) {
        pager().FreePending(iter->first);
    } else {
        pager().FreePending(kTxInvalidId);
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

void TxManager::Continue() {
    assert(!update_tx_.has_value());
    update_tx_.emplace(this, db_->meta().meta_format(), true);
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
    CopyMetaInfo(&db_->meta().meta_format(), update_tx_->meta_format());

    // 不在此处保存
    //pager().SyncAllPage();
    //db_->meta().Switch();
    //db_->meta().Save();

    AppendCommitLog();
    update_tx_ = std::nullopt;
}

TxImpl& TxManager::update_tx() {
    assert(update_tx_.has_value());
    return *update_tx_;
}

void TxManager::AppendPutLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    BucketLogHeader format;
    format.type = OperationType::kPut;
    format.bucket_id = bucket_id;
    std::array<std::span<const uint8_t>, 3> arr;
    arr[0] = { reinterpret_cast<const uint8_t*>(&format), kBucketPutLogHeaderSize };
    arr[1] = key;
    arr[2] = value;
    db_->AppendLog(arr.begin(), arr.end());
}

void TxManager::AppendInsertLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
    BucketLogHeader format;
    format.type = OperationType::kInsert;
    format.bucket_id = bucket_id;
    std::array<std::span<const uint8_t>, 3> arr;
    arr[0] = { reinterpret_cast<const uint8_t*>(&format), kBucketInsertLogHeaderSize };
    arr[1] = key;
    arr[2] = value;
    db_->AppendLog(arr.begin(), arr.end());
}

void TxManager::AppendDeleteLog(BucketId bucket_id, std::span<const uint8_t> key) {
    BucketLogHeader format;
    format.type = OperationType::kDelete;
    format.bucket_id = bucket_id;
    std::array<std::span<const uint8_t>, 2> arr;
    arr[0] = { reinterpret_cast<const uint8_t*>(&format), kBucketDeleteLogHeaderSize };
    arr[1] = key;
    db_->AppendLog(arr.begin(), arr.end());
}

Pager& TxManager::pager() const {
    return db_->pager();
}

void TxManager::AppendBeginLog() {
    OperationType type = OperationType::kBegin;
    std::array<std::span<const uint8_t>, 1> arr;
    arr[0] = { reinterpret_cast<const uint8_t*>(&type), sizeof(type) };
    db_->AppendLog(arr.begin(), arr.end());
}

void TxManager::AppendRollbackLog() {
    OperationType type = OperationType::kRollback;
    std::array<std::span<const uint8_t>, 1> arr;
    arr[0] = { reinterpret_cast<const uint8_t*>(&type), sizeof(type) };
    db_->AppendLog(arr.begin(), arr.end());
}

void TxManager::AppendCommitLog() {
    OperationType type = OperationType::kCommit;
    std::array<std::span<const uint8_t>, 1> arr;
    arr[0] = { reinterpret_cast<const uint8_t*>(&type), sizeof(type) };
    db_->AppendLog(arr.begin(), arr.end());
    db_->log_writer().WriteBuffer();
}


} // namespace yudb