#include "yudb/db_impl.h"

#include "tinyio/tinyio.hpp"
#include "yudb/crc32.h"
#include "yudb/error.h"
#include "yudb/version.h"

namespace yudb{

 DB::~DB() = default;

 std::unique_ptr<DB> DB::Open(const Options& options, const std::string_view path) {
     auto db = std::make_unique<DBImpl>();
     db->options_.emplace(options);
     if (db->options_->page_size == 0) {
         auto page_size = mio::page_size();
         if (page_size > kPageMaxSize) {
             throw IoError{ "the system page size exceeds the range." };
         }
         db->options_->page_size = static_cast<PageSize>(page_size);
     } else {
         if (db->options_->page_size != mio::page_size()) {
             throw InvalidArgumentError{ "page size mismatch." };
         }
     }

     db->db_path_ = path;
     db->db_file_.open(path, tinyio::access_mode::write);
     db->db_file_.lock(tinyio::share_mode::exclusive);
     bool init_meta = false;
     if (!db->options_->read_only) {
         if (db->db_file_.size() == 0) {
             db->db_file_.resize(db->options_->page_size * kPageInitCount);
             init_meta = true;
         }
     }
     db->InitDBFile();
     db->InitShmFile();
     db->meta_.emplace(db.get(), &db->shm_->meta_struct());
     if (init_meta) {
         db->meta_->Init();
     } else {
         db->meta_->Load();
     }
     db->pager_.emplace(db.get(), db->options_->page_size);
     db->tx_manager_.emplace(db.get());

     db->InitLogFile();
     if (db->options_->read_only) {
         db->db_file_.unlock();
         db->db_file_.lock(tinyio::share_mode::shared);
     }
     return db;
 }


DBImpl::~DBImpl() {
     if (options_.has_value()) {
         logger_.reset();
         tx_manager_.reset();
         pager_.reset();
         uint64_t new_size = options_->page_size * meta_->meta_struct().page_count;
         meta_.reset();
         shm_.reset();

         db_mmap_.unmap();
         shm_mmap_.unmap();
         if (!options_->read_only) {
             std::filesystem::remove(db_path_ + "-shm");
         }
         db_file_.resize(new_size);
         db_file_.unlock();
     }
 }

UpdateTx DBImpl::Update() {
    if (options_->read_only) {
        throw InvalidArgumentError{ "the database is read-only." };
    }
    return UpdateTx{ &tx_manager_->Update(options_->defaluit_comparator) };
 }

ViewTx DBImpl::View() {
     return tx_manager_->View(options_->defaluit_comparator);
 }


void DBImpl::Remmap(uint64_t new_size) {
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
    if (db_mmap_pending_.empty()) return;
     const std::unique_lock lock{ db_mmap_lock_ };
     for (auto& mmap : db_mmap_pending_) {
         mmap.unmap();
     }
     db_mmap_pending_.clear();
 }

void DBImpl::InitDBFile() {
    std::error_code error_code;
    db_mmap_ = mio::make_mmap_sink(db_path_, error_code);
    if (error_code) {
        throw IoError{ "unable to map db file." };
    }
}

void DBImpl::InitShmFile() {
    const std::string shm_path = db_path_ + "-shm";
    tinyio::file shm_file;
    shm_file.open(shm_path, tinyio::access_mode::write);
    bool init_shm = false;
    if (shm_file.size() < sizeof(ShmStruct)) {
        shm_file.resize(sizeof(ShmStruct));
        init_shm = true;
    }
    if (std::filesystem::exists(db_path_ + "-wal")) {
        init_shm = true;
    }
    std::error_code error_code;
    shm_mmap_ = mio::make_mmap_sink(shm_path, error_code);
    if (error_code) {
        throw IoError{ "unable to map shm file." };
    }
    shm_.emplace(reinterpret_cast<ShmStruct*>(shm_mmap_.data()));
    if (init_shm) {
        shm_->Recover();
    }
    
}

void DBImpl::InitLogFile() {
    if (options_->read_only) {
        return;
    }
    logger_.emplace(this, db_path_ + "-wal");
    if (logger_->RecoverNeeded()) {
        logger_->Recover();
        logger_->Reset();
    }
    logger_->AppendPersistedLog();
}

} // namespace yudb