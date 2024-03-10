#include "yudb/logger.h"

#include "yudb/db_impl.h"
#include "yudb/log_reader.h"
#include "yudb/tx.h"

namespace yudb{

Logger::Logger(DBImpl* db, std::string_view log_path) : 
    db_{ db },
    log_path_{ log_path }
{
    writer_.Open(log_path);
}

Logger::~Logger() {
    if (!db_->options()->read_only) {
        Checkpoint();
        writer_.Close();
        std::filesystem::remove(log_path_);
    }
}

void Logger::AppendLog(const std::span<const uint8_t>* begin, const std::span<const uint8_t>* end) {
    if (recovering_) return;

    for (auto it = begin; it != end; ++it) {
        writer_.AppendRecordToBuffer(*it);
    }
    if (!need_checkpoint_ && writer_.size() >= db_->options()->max_wal_size) {
        need_checkpoint_ = true;
    }
}

void Logger::FlushLog() {
    if (recovering_) return;
    writer_.FlushBuffer();
}

void Logger::Reset() {
    writer_.Close();
    writer_.Open(log_path_);
}

bool Logger::NeedRecover() {
    return writer_.file().size() > 0;
}

void Logger::Recover() {
    recovering_ = true;
    yudb::log::Reader reader;
    reader.Open(log_path_);
    std::optional<UpdateTx> current_tx;
    bool end = false, init = false;
    auto& meta = db_->meta();
    auto& pager = db_->pager();
    auto& tx_manager = db_->tx_manager();
    auto& raw_txid = meta.meta_struct().txid;
    do {
        if (end) {
            break;
        }
        auto record = reader.ReadRecord();
        if (!record) {
            break;
        }
        if (record->size() < sizeof(LogType)) {
            break;
        }
        auto type = *reinterpret_cast<LogType*>(record->data());
        switch (type) {
        case LogType::kPersisted: {
            if (init) {
                throw LoggerError{ "abnormal logging." };
            }
            auto log = reinterpret_cast<PersistedLogHeader*>(record->data());
            if (meta.meta_struct().txid > log->txid) {
                end = true;
            }
            init = true;
            break;
        }
        case LogType::kBegin: {
            if (!init) {
                throw LoggerError{ "abnormal logging." };
            }
            if (current_tx.has_value()) {
                throw LoggerError{ "abnormal logging." };
            }
            current_tx.emplace(&tx_manager.Update());
            break;
        }
        case LogType::kRollback: {
            if (!init) {
                throw LoggerError{ "abnormal logging." };
            }
            if (!current_tx.has_value()) {
                throw LoggerError{ "abnormal logging." };
            }
            current_tx->RollBack();
            current_tx = std::nullopt;
            break;
        }
        case LogType::kCommit: {
            if (!init) {
                throw LoggerError{ "abnormal logging." };
            }
            if (!current_tx.has_value()) {
                throw LoggerError{ "abnormal logging." };
            }
            current_tx->Commit();
            current_tx = std::nullopt;
            break;
        }
        case LogType::kPut: {
            if (!init) {
                throw LoggerError{ "abnormal logging." };
            }
            assert(record->size() == kBucketPutLogHeaderSize);
            auto log = reinterpret_cast<BucketLogHeader*>(record->data());
            auto& tx = tx_manager.update_tx();
            BucketImpl* bucket;
            if (log->bucket_id == kUserRootBucketId) {
                bucket = &tx.user_bucket();
            } else {
                bucket = &tx.AtSubBucket(log->bucket_id);
            }
            auto key = reader.ReadRecord();
            if (!key) {
                end = true;
                break;
            }
            auto value = reader.ReadRecord();
            if (!value) {
                end = true;
                break;
            }
            bucket->Put(key->data(), key->size(), value->data(), value->size(), false);
            break;
        }
        case LogType::kDelete: {
            if (!init) {
                throw LoggerError{ "abnormal logging." };
            }
            assert(record->size() == kBucketDeleteLogHeaderSize);
            auto log = reinterpret_cast<BucketLogHeader*>(record->data());
            auto& tx = tx_manager.update_tx();
            auto& bucket = tx.AtSubBucket(log->bucket_id);
            auto key = reader.ReadRecord();
            if (!key) {
                end = true;
                break;
            }
            bucket.Delete(key->data(), key->size());
            break;
        }
        }
    } while (true);
    recovering_ = false;
    pager.WriteAllDirtyPages();
    if (current_tx.has_value()) {
        // 不完整的日志记录，丢弃最后的事务
        current_tx->RollBack();
    }
    if (meta.meta_struct().txid > raw_txid) {
        std::error_code error_code;
        db_->db_file_mmap().sync(error_code);
        if (error_code) {
            throw IoError{ "failed to sync db file." };
        }
        meta.Switch();
        meta.Save();
    }
}

void Logger::Checkpoint() {
    auto& meta = db_->meta();
    auto& pager = db_->pager();
    auto& tx_manager = db_->tx_manager();

    if (tx_manager.has_update_tx()) {
        throw CheckpointError{ "checkpoint execution is not allowed when there is a write transaction." };
    }

    auto tx = db_->Update();
    db_->pager().SaveFreeList();
    tx.Commit();

    pager.WriteAllDirtyPages();
    std::error_code error_code;
    db_->db_file_mmap().sync(error_code);
    if (error_code) {
        throw IoError{ "failed to sync db file." };
    }
    meta.Switch();
    meta.Save();
    Reset();
    AppendPersistedLog();
}

void Logger::AppendPersistedLog() {
    PersistedLogHeader log;
    log.type = LogType::kPersisted;
    log.txid = db_->meta().meta_struct().txid;
    std::span<const uint8_t> arr[1];
    arr[0] = { reinterpret_cast<const uint8_t*>(&log), sizeof(PersistedLogHeader) };
    AppendLog(std::begin(arr), std::end(arr));
}

} // namespace yudb