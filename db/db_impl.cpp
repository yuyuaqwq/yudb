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
     bool db_init = false;
     if (db->db_file_.size() < mio::page_size() * kPageInitCount) {
         db->db_file_.resize(mio::page_size() * kPageInitCount);
         db_init = true;
     }
     db->db_file_.unlock();
     std::error_code error_code;
     db->db_mmap_ = mio::make_mmap_sink(db->db_path_, error_code);
     if (error_code) {
         throw IoError{ "unable to map db file." };
     }

     std::string shm_path = path.data();
     shm_path += "-shm";
     tinyio::file shm_file;
     shm_file.open(shm_path, tinyio::access_mode::write);
     shm_file.lock(tinyio::share_mode::exclusive);
     bool shm_init = false;
     if (shm_file.size() == 0) {
         shm_file.resize(mio::page_size());
     }
     shm_file.unlock();
     db->shm_mmap_ = mio::make_mmap_sink(shm_path, error_code);
     if (error_code) {
         throw IoError{ "unable to map shm file." };
     }
     db->shm_.emplace(reinterpret_cast<ShmStruct*>(db->shm_mmap_.data()));
     if (shm_init) {
         db->shm_->Init();
     }

     db->pager_.emplace(db.get(), db->options_->page_size);
     if (db_init) {
         db->Init();
     }
     if (!db->meta().Load()) {
         return {};
     }

     std::string log_path = path.data();
     log_path += "-wal";
     tinyio::file log_file;
     log_file.open(log_path, tinyio::access_mode::write);
     log_file.lock(tinyio::share_mode::exclusive);
     if (log_file.size() > 0) {
         db->shm_->Init();
         db->Recover(log_path);
         log_file.resize(0);
     }
     log_file.unlock();
     log_file.close();

     db->log_writer().Open(log_path);
     return db;
 }


 DBImpl::~DBImpl() {
     if (pager_.has_value()) {
         auto tx = Update();
         Checkpoint();
         tx.Commit();
         
         db_mmap_.unmap();
         shm_mmap_.unmap();
         std::filesystem::remove(db_path_ + "-shm");
         log_writer_.Close();
         std::filesystem::remove(log_writer_.path());
     }
 }

UpdateTx DBImpl::Update() {
     return tx_manager_.Update();
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
     meta_.Set(tx.meta_format());

     pager_->WriteAllDirtyPages();
     meta_.Switch();
     meta_.Save();
     std::error_code error_code;
     db_mmap_.sync(error_code);
     if (error_code) {
         throw IoError{ "failed to sync db file." };
     }

     log_writer_.Reset();
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

void DBImpl::Init() {
    auto ptr = db_mmap_.data();
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
    db_mmap_.sync(error_code);
    if (error_code) {
        throw IoError{ "failed to sync db file." };
    }
}

} // namespace yudb