#include "yudb/db_impl.h"

#include "third_party/tinyio.hpp"
#include "yudb/crc32.h"
#include "yudb/error.h"
#include "yudb/log_reader.h"
#include "yudb/operator_log_format.h"
#include "yudb/version.h"

namespace yudb{

namespace fs = std::filesystem;

 DB::~DB() = default;

 std::unique_ptr<DB> DB::Open(const Options& options, std::string_view path) {
     auto db = std::make_unique<DBImpl>();
     db->options_.emplace(options);
     if (db->options_->page_size == 0) {
         db->options_->page_size = mio::page_size();
     } else {
         if (db->options_->page_size != mio::page_size()) {
             return {};
         }
     }

     db->db_path_ = path;
     db->db_file_.open(path, tinyio::access_mode::write);
     db->db_file_.lock(tinyio::share_mode::exclusive);
     bool init_meta = false;
     if (!db->options_->read_only) {
         if (db->db_file_.size() == 0) {
             db->db_file_.resize(mio::page_size() * kPageInitCount);
             init_meta = true;
         }
     }
     db->InitDBMmap();
     db->InitShmMmap();
     db->meta_.emplace(db.get(), &db->shm_->meta_struct());
     if (init_meta) {
         db->meta_->Init();
     } else {
         if (!db->meta_->Load()) {
             return {};
         }
     }
     db->pager_.emplace(db.get(), db->options_->page_size);

     db->InitLog();
     if (db->options_->read_only) {
         db->db_file_.unlock();
         db->db_file_.lock(tinyio::share_mode::shared);
     }
     return db;
 }


 DBImpl::~DBImpl() {
     if (options_.has_value()) {
         if (!options_->read_only) {
             auto tx = Update();
             Checkpoint();
             tx.Commit();
         }
         db_mmap_.unmap();
         shm_mmap_.unmap();
         if (!options_->read_only) {
             std::filesystem::remove(db_path_ + "-shm");
             log_writer_.Close();
             std::filesystem::remove(log_writer_.path());
         }
         db_file_.unlock();
     }
 }

UpdateTx DBImpl::Update() {
    if (options_->read_only) {
        throw InvalidArgumentError{ "the database is read-only." };
    }
    return UpdateTx{ &tx_manager_.Update() };
 }

ViewTx DBImpl::View() {
     db_mmap_lock_.lock_shared();
     return tx_manager_.View();
 }

void DBImpl::Checkpoint() {
     if (!tx_manager_.has_update_tx()) {
         throw CheckpointError{ "checkpoint execution is not allowed when there is a write transaction." };
     }

     pager_->SaveFreeList();
     auto& tx = tx_manager_.update_tx();
     meta_->Reset(tx.meta_format());

     pager_->WriteAllDirtyPages();
     std::error_code error_code;
     db_mmap_.sync(error_code);
     if (error_code) {
         throw IoError{ "failed to sync db file." };
     }
     meta_->Switch();
     meta_->Save();
     log_writer_.Reset();
     AppendInitLog();
 }

void DBImpl::Mmap(uint64_t new_size) {
     db_mmap_pending_.emplace_back(std::move(db_mmap_));
     // 1GB之前二倍扩展
     uint64_t map_size;
     const uint64_t max_expand_size = 1024 * 1024 * 1024;
     if (new_size <= max_expand_size) {
         map_size = 1;
         for (uint32_t i = 0; i < 31; ++i) {
             map_size *= 2;
             if (map_size > new_size) {
                 break;
             }
         }
     } else {
         map_size = new_size + (new_size % max_expand_size);
     }
     assert(map_size % pager_->page_size() == 0);

     db_file_.resize(map_size);
     std::error_code ec;
     db_mmap_.map(db_path_, ec);
     if (ec) {
         throw IoError{ "unable to map db file."};
     }
 }

void DBImpl::ClearMmap() {
    std::unique_lock lock{ db_mmap_lock_ };
     for (auto& mmap : db_mmap_pending_) {
         mmap.unmap();
     }
     db_mmap_pending_.clear();
 }

void DBImpl::Recover(std::string_view path) {
    recovering_ = true;
    yudb::log::Reader reader;
    reader.Open(path);
    std::optional<UpdateTx> current_tx;
    bool end = false, init = false;
    do {
        if (end) {
            break;
        }
        auto record = reader.ReadRecord();
        if (!record) {
            break;
        }
        if (record->size() < sizeof(OperationType)) {
            break;
        }
        auto type = *reinterpret_cast<OperationType*>(record->data());
        switch (type) {
        case OperationType::kInit: {
            auto format = reinterpret_cast<InitLogHeader*>(record->data());
            if (format->txid <= meta_->meta_struct().txid) {
                end = true;
            }
            init = true;
            break;
        }
        case OperationType::kBegin: {
            if (!init) {
                throw RecoverError{ "abnormal logging." };
            }
            if (current_tx.has_value()) {
                throw RecoverError{ "abnormal logging." };
            }
            current_tx.emplace(&tx_manager_.Update());
            break;
        }
        case OperationType::kRollback: {
            if (!init) {
                throw RecoverError{ "abnormal logging." };
            }
            if (!current_tx.has_value()) {
                throw RecoverError{ "abnormal logging." };
            }
            current_tx->RollBack();
            current_tx = std::nullopt;
            break;
        }
        case OperationType::kCommit: {
            if (!init) {
                throw RecoverError{ "abnormal logging." };
            }
            if (!current_tx.has_value()) {
                throw RecoverError{ "abnormal logging." };
            }
            current_tx->Commit();
            current_tx = std::nullopt;
            break;
        }
        case OperationType::kPut: {
            if (!init) {
                throw RecoverError{ "abnormal logging." };
            }
            assert(record->size() == kBucketPutLogHeaderSize);
            auto format = reinterpret_cast<BucketLogHeader*>(record->data());
            auto& tx = tx_manager_.update_tx();
            BucketImpl* bucket;
            if (format->bucket_id == kUserRootBucketId) {
                bucket = &tx.user_bucket();
            } else {
                bucket = &tx.AtSubBucket(format->bucket_id);
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
            bucket->Put(key->data(), key->size(), value->data(), value->size());
            break;
        }
        case OperationType::kDelete: {
            if (!init) {
                throw RecoverError{ "abnormal logging." };
            }
            assert(record->size() == kBucketDeleteLogHeaderSize);
            auto format = reinterpret_cast<BucketLogHeader*>(record->data());
            auto& tx = tx_manager_.update_tx();
            auto& bucket = tx.AtSubBucket(format->bucket_id);
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
    pager_->WriteAllDirtyPages();
    if (current_tx.has_value()) {
        // 不完整的日志记录，丢弃最后的事务
        current_tx->RollBack();
    }
    std::error_code error_code;
    db_mmap_.sync(error_code);
    if (error_code) {
        throw IoError{ "failed to sync db file." };
    }
    meta_->Switch();
    meta_->Save();
}

void DBImpl::AppendInitLog() {
    InitLogHeader format;
    format.type = OperationType::kInit;
    format.txid = meta_->meta_struct().txid + 1;
    std::array<std::span<const uint8_t>, 1> arr;
    arr[0] = { reinterpret_cast<const uint8_t*>(&format), sizeof(InitLogHeader)};
    AppendLog(arr.begin(), arr.end());
}

void DBImpl::InitDBMmap() {
    std::error_code error_code;
    db_mmap_ = mio::make_mmap_sink(db_path_, error_code);
    if (error_code) {
        throw IoError{ "unable to map db file." };
    }
}

void DBImpl::InitShmMmap() {
    std::string shm_path = db_path_ + "-shm";
    tinyio::file shm_file;
    shm_file.open(shm_path, tinyio::access_mode::write);
    bool init_shm = false;
    if (shm_file.size() < sizeof(ShmStruct)) {
        shm_file.resize(sizeof(ShmStruct));
        init_shm = true;
    }
    std::error_code error_code;
    shm_mmap_ = mio::make_mmap_sink(shm_path, error_code);
    if (error_code) {
        throw IoError{ "unable to map shm file." };
    }
    shm_.emplace(reinterpret_cast<ShmStruct*>(shm_mmap_.data()));
    if (init_shm) {
        shm_->Init();
    }
}

void DBImpl::InitLog() {
    if (options_->read_only) {
        return;
    }
    std::string log_path = db_path_ + "-wal";
    tinyio::file log_file;
    log_file.open(log_path, tinyio::access_mode::write);
    if (log_file.size() > 0) {
        shm_->Init();
        Recover(log_path);
        log_file.resize(0);
    }
    log_writer().Open(log_path);
    AppendInitLog();
}

} // namespace yudb