//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include "tx_manager.h"

#include "db_impl.h"
#include "log_type.h"

namespace yudb {

TxManager::TxManager(DBImpl* db)
    : db_(db)
{
    pager().LoadFreeList();
    min_view_txid_ = db_->meta().meta_struct().txid;
}

TxManager::~TxManager() {
    auto lock = std::unique_lock(db_->shm()->meta_lock());

    if (update_tx_.has_value()
        || !view_tx_map_.empty()) {
        // throw std::runtime_error("There are write transactions that have not been exited.");
        std::abort();
    }
}

UpdateTx TxManager::Update() {
    db_->shm()->update_lock().lock();
    db_->ClearMmap();

    assert(!update_tx_.has_value());

    if (db_->options()->mode == DbMode::kWal) {
        AppendBeginLog();
    }
    

    auto lock = std::unique_lock(db_->shm()->meta_lock());

    update_tx_.emplace(this, db_->meta().meta_struct(), true);
    update_tx_->set_txid(update_tx_->txid() + 1);
    if (update_tx_->txid() == kTxInvalidId) {
        throw std::runtime_error("Txid overflow.");
    }

    // 更新min_view_txid
    if (view_tx_map_.empty()) {
        min_view_txid_ = db_->meta().meta_struct().txid;
    } else {
        const auto iter = view_tx_map_.cbegin();
        assert(iter != view_tx_map_.end());
        min_view_txid_ = iter->first;
    }
    pager().Release(min_view_txid_ - 1);
    return UpdateTx(&*update_tx_);
}

ViewTx TxManager::View() {
    auto lock = std::unique_lock(db_->shm()->meta_lock());

    auto txid = db_->meta().meta_struct().txid;
    const auto iter = view_tx_map_.find(txid);
    if (iter == view_tx_map_.end()) {
        view_tx_map_.insert({ txid, 1});
    } else {
        ++iter->second;
    }
    return ViewTx(this, db_->meta().meta_struct(), &db_->db_file_mmap_lock());
}

void TxManager::RollBack() {
    auto lock = std::unique_lock(db_->shm()->meta_lock());

    if (db_->options()->mode == DbMode::kWal) {
        AppendRollbackLog();
    }
    
    if (db_->logger().CheckPointNeeded()) {
        db_->logger().Checkpoint();
    }
    pager().Rollback();
    update_tx_ = std::nullopt;
    db_->shm()->update_lock().unlock();
}

void TxManager::RollBack(TxId view_txid) {
    auto lock = std::unique_lock(db_->shm()->meta_lock());

    const auto iter = view_tx_map_.find(view_txid);
    assert(iter != view_tx_map_.end());
    assert(iter->second > 0);
    --iter->second;
    if (iter->second == 0) {
        view_tx_map_.erase(iter);
    }
}

void TxManager::Commit() {
    auto lock = std::unique_lock(db_->shm()->meta_lock());

    db_->meta().Reset(update_tx_->meta_struct());

    if (db_->options()->mode == DbMode::kWal) {
        AppendCommitLog();
        if (db_->logger().CheckPointNeeded()) {
            db_->logger().Checkpoint();
        }
    }
    else if (db_->options()->mode == DbMode::kUpdateInPlace) {
        db_->pager().SaveFreeList();
        db_->pager().WriteAllDirtyPages();

        db_->meta().Switch();
        db_->meta().Save();
    }

    update_tx_ = std::nullopt;
    db_->shm()->update_lock().unlock();
}

bool TxManager::IsTxExpired(TxId txid) const {
    return txid < min_view_txid_;
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
    if (db_->options()->mode != DbMode::kWal) {
        return;
    }

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
    if (db_->options()->mode != DbMode::kWal) {
        return;
    }

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

Pager& TxManager::pager() const {
    return db_->pager();
}

TxImpl& TxManager::update_tx() {
    assert(update_tx_.has_value());
    return *update_tx_;
}

TxImpl& TxManager::view_tx(ViewTx* view_tx) {
    return view_tx->tx_;
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