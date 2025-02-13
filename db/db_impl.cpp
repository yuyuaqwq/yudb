//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include "db_impl.h"

#include <wal/tinyio.hpp>
#include <wal/crc32.h>

#include <yudb/version.h>

namespace yudb{

DB::DB() = default;
DB::~DB() = default;

std::unique_ptr<DB> DB::Open(const Options& options, const std::string_view path) {
    auto db = std::make_unique<DBImpl>();

    db->options_.emplace(options);

    auto& db_options = *db->options();
    auto& db_file = db->db_file();

    auto page_size = mio::page_size();
    if (page_size > kPageMaxSize) {
        throw std::runtime_error("Page size exceeds maximum limit: " + std::to_string(page_size));
    }
    if (db_options.page_size == 0) {
        db_options.page_size = static_cast<PageSize>(page_size);
    } else {
        if (db_options.page_size != mio::page_size()) {
            throw std::invalid_argument("Options page size mismatch.");
        }
    }

    db->db_path_ = path;
    db_file.open(db->db_path_, tinyio::access_mode::sync_needed);
    db_file.lock(tinyio::share_mode::exclusive);

    bool init_meta = false;
    if (!db_options.read_only) {
        if (db_file.size() == 0) {
            db_file.resize(db->options_->page_size * kPageInitCount);
            init_meta = true;
        }
    }

    db->InitDBFile();

    db->InitShmFile();

    db->meta_.emplace(db.get(), &db->shm_->meta_struct());
    auto& db_meta = db->meta();

    if (init_meta) {
        db_meta.Init();
    } else {
        db_meta.Load();
    }

    db->pager_.emplace(db.get(), db->options_->page_size);

    db->tx_manager_.emplace(db.get());

    if (db_options.mode == DbMode::kWal) {
        db->InitLogFile();
    }
    
    if (db_options.read_only) {
        db_file.unlock();
        db_file.lock(tinyio::share_mode::shared);
    }
    return db;
}

DBImpl::~DBImpl() {
    if (!options_.has_value()) {
        return;
    }

    logger_.reset();
    tx_manager_.reset();
    pager_.reset();

    uint64_t new_size = 0;
    if (meta_.has_value()) {
        new_size = options_->page_size * meta_->meta_struct().page_count;
    }

    meta_.reset();
    shm_.reset();

    if (db_mmap_.is_mapped()) {
        db_mmap_.unmap();
    }

    if (shm_mmap_.is_mapped()) {
        shm_mmap_.unmap();
    }

    if (!options_->read_only) {
        std::error_code ec;
        std::filesystem::remove(db_path_ + "-shm", ec);
    }

    if (db_file_.is_open()) {
        if (new_size > 0) {
            db_file_.resize(new_size);
        }
        db_file_.unlock();
    }
 }

UpdateTx DBImpl::Update() {
    if (options_->read_only) {
        throw std::invalid_argument("the database is read-only.");
    }
    return tx_manager_->Update();
 }

ViewTx DBImpl::View() {
    return tx_manager_->View();
 }

void DBImpl::Remmap(uint64_t new_size) {
     db_mmap_pending_.emplace_back(std::move(db_mmap_));
     // Double expansion before 1GB
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
         throw std::system_error(ec, "Unable to map db file.");
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
    std::error_code ec;
    db_mmap_ = mio::make_mmap_sink(db_path_, ec);
    if (ec) {
        throw std::system_error(ec, "Unable to map db file.");
    }
}

void DBImpl::InitShmFile() {
    const std::string shm_path = db_path_ + "-shm";
    std::error_code ec;
    std::filesystem::remove(shm_path, ec);
    tinyio::file shm_file;
    shm_file.open(shm_path, tinyio::access_mode::write);
    if (shm_file.size() < sizeof(ShmStruct)) {
        shm_file.resize(sizeof(ShmStruct));
    }
    shm_mmap_ = mio::make_mmap_sink(shm_path, ec);
    if (ec) {
        throw std::system_error(ec, "Unable to map shm file.");
    }
    shm_.emplace(reinterpret_cast<ShmStruct*>(shm_mmap_.data()));
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
    logger_->AppendWalTxIdLog();
}

} // namespace yudb