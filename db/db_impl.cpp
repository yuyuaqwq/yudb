#include "yudb/db_impl.h"

#include <fstream>
#include <filesystem>

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

     std::error_code error_code;
     db->db_file_.open(path, tinyio::access_mode::write, error_code);
     if (error_code) {
         throw IoError{ "unable to open db file." };
     }
     db->db_file_.lock(tinyio::share_mode::exclusive, error_code);
     if (error_code) {
         throw IoError{ "unable to lock db file." };
     }
     auto size = db->db_file_.size(error_code);
     if (error_code) {
         throw IoError{ "unable to query size db file." };
     }

     bool init = false;
     if (size < mio::page_size() * kPageInitCount) {
         db->db_file_.resize(mio::page_size() * kPageInitCount, error_code);
         if (error_code) {
             throw IoError{ "unable to resize db file." };
         }
         init = true;
     }
     db->db_file_.unlock(error_code);
     if (error_code) {
         throw IoError{ "unable to unlock db file." };
     }

     db->db_path_ = path;
     db->db_file_mmap_ = mio::make_mmap_sink(db->db_path_, error_code);
     if (error_code) {
         throw IoError{ "unable to map db file." };
     }
     db->pager_.emplace(db.get(), db->options_->page_size);

     if (init) {
         db->InitMeta();
     }
     if (!db->meta().Load()) {
         return {};
     }
     std::string log_path = path.data();
     log_path += "-wal";

     namespace fs = std::filesystem;
     auto file_status = fs::status(path, error_code);
     if (fs::exists(log_path)) {
         db->Recover(log_path);
         fs::remove(log_path);
     }
     db->log_writer().Open(log_path);
     return db;
 }


 DBImpl::~DBImpl() {
     if (pager_.has_value()) {
         pager_->WriteAllDirtyPages();
         meta_.Switch();
         meta_.Save();
         std::error_code code;
         db_file_mmap_.sync(code);
         if (code) {
             throw IoError{ "db file sync error." };
         }
         log_writer_.Close();
         std::filesystem::remove(log_writer_.path());
     }
 }

 UpdateTx DBImpl::Update() {
     return tx_manager_.Update();
 }

 ViewTx DBImpl::View() {
     db_file_mmap_lock_.lock_shared();
     return tx_manager_.View();
 }

 void DBImpl::Checkpoint() {
     if (!tx_manager_.has_update_tx()) {
         throw CheckpointError{ "checkpoint execution is not allowed when there is a write transaction." };
     }

     pager_->UpdateFreeList();
     auto& tx = tx_manager_.update_tx();
     CopyMetaInfo(&meta_.meta_struct(), tx.meta_format());

     pager_->WriteAllDirtyPages();
     meta_.Switch();
     meta_.Save();
     std::error_code error_code;
     db_file_mmap_.sync(error_code);
     if (error_code) {
         throw IoError{ "failed to sync db file." };
     }

     log_writer_.Reset();
 }

 void DBImpl::Mmap(uint64_t new_size) {
     std::unique_lock lock{ db_file_mmap_lock_ };
     db_file_mmap_.unmap();
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
     }
     else {
         map_size = new_size + (new_size % max_expand_size);
     }
     assert(map_size % pager_->page_size() == 0);

     std::error_code ec;
     db_file_.resize(map_size, ec);
     if (ec) {
         throw IoError{ "unable to resize db file." };
     }
     db_file_mmap_.map(db_path_, ec);
     if (ec) {
         throw IoError{ "unable to map db file."};
     }
 }

void DBImpl::Recover(std::string_view path) {
    // 实际上的恢复过程还需要判断是否已经进行了恢复但中途又发生了崩溃
    recovering_ = true;
    yudb::log::Reader reader;
    reader.Open(path);
    std::optional<UpdateTx> current_tx;
    do {
        auto record = reader.ReadRecord();
        if (!record) {
            break;
        }
        if (record->size() < sizeof(OperationType)) {
            break;
        }
        bool end = false;
        auto type = *reinterpret_cast<OperationType*>(record->data());
        switch (type) {
        case OperationType::kBegin: {
            if (current_tx.has_value()) {
                throw RecoverError{ "abnormal logging." };
            }
            current_tx = Update();
            break;
        }
        case OperationType::kRollback: {
            if (!current_tx.has_value()) {
                throw RecoverError{ "abnormal logging." };
            }
            current_tx->RollBack();
            current_tx = std::nullopt;
            break;
        }
        case OperationType::kCommit: {
            if (!current_tx.has_value()) {
                throw RecoverError{ "abnormal logging." };
            }
            current_tx->Commit();
            current_tx = std::nullopt;
            break;
        }
        case OperationType::kPut: {
            assert(record->size() == kBucketPutLogHeaderSize);
            auto format = reinterpret_cast<BucketLogHeader*>(record->data());
            auto& tx = tx_manager_.update_tx();
            auto& bucket = tx.AtSubBucket(format->bucket_id);
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
            bucket.Put(key->data(), key->size(), value->data(), value->size());
            break;
        }
        case OperationType::kDelete: {
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
    meta_.Save();
    std::error_code error_code;
    db_file_mmap_.sync(error_code);
    if (error_code) {
        throw IoError{ "failed to sync db file." };
    }
}

void DBImpl::InitMeta() {
    auto ptr = db_file_mmap_.data();
    auto first = reinterpret_cast<MetaStruct*>(ptr);
    first->sign = YUDB_SIGN;
    first->page_size = mio::page_size();
    first->min_version = YUDB_VERSION;
    first->page_count = 2;
    first->txid = 1;
    first->user_root = kPageInvalidId;
    first->free_list_pgid = kPageInvalidId;
    first->free_pair_count = 0;
    first->free_list_page_count = 0;

    Crc32 crc32;
    crc32.Append(first, kMetaSize - sizeof(uint32_t));
    first->crc32 = crc32.End();

    auto second = reinterpret_cast<MetaStruct*>(ptr + pager_->page_size());
    std::memcpy(second, first, kMetaSize - sizeof(uint32_t));
    second->txid = 2;

    crc32.Clear();
    crc32.Append(second, kMetaSize - sizeof(uint32_t));
    second->crc32 = crc32.End();
}



} // namespace yudb