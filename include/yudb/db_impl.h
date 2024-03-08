#pragma once

#include <string>
#include <optional>
#include <memory>
#include <shared_mutex>

#include "third_party/mio.hpp"
#include "yudb/db.h"
#include "yudb/logger.h"
#include "yudb/meta.h"
#include "yudb/pager.h"
#include "yudb/tx_manager.h"
#include "yudb/shm.h"

namespace yudb {

class DBImpl : public DB {
public:
    DBImpl() = default;
    ~DBImpl() override;

    UpdateTx Update() override;
    ViewTx View() override;

    void Mmap(uint64_t new_size);
    void ClearMmap();

    auto& options() const { return options_; }
    auto& options() { return options_; }
    auto& db_file() const { return db_file_; }
    auto& db_file() { return db_file_; }
    auto& db_file_mmap() const { return db_mmap_; }
    auto& db_file_mmap() { return db_mmap_; }
    auto& db_file_mmap_lock() const { return db_mmap_lock_; }
    auto& db_file_mmap_lock() { return db_mmap_lock_; }
    auto& shm() const { return shm_; }
    auto& shm() { return shm_; }
    auto& meta() const { assert(meta_.has_value()); return *meta_; }
    auto& meta() { assert(meta_.has_value()); return *meta_; }
    auto& pager() const { assert(pager_.has_value()); return *pager_; }
    auto& pager() { assert(pager_.has_value()); return *pager_; }
    auto& tx_manager() const { assert(tx_manager_.has_value()); return *tx_manager_; }
    auto& tx_manager() { assert(tx_manager_.has_value()); return *tx_manager_; }
    auto& logger() const { return logger_; }
    auto& logger() { return logger_; }

private:
    void InitDBFile();
    void InitShmFile();
    void InitLogFile();
    
private:
    friend class DB;

    std::optional<Options> options_;

    std::string db_path_;
    tinyio::file db_file_;
    mio::mmap_sink db_mmap_;
    std::shared_mutex db_mmap_lock_;
    std::vector<mio::mmap_sink> db_mmap_pending_;

    mio::mmap_sink shm_mmap_;
    std::optional<Shm> shm_;

    std::optional<Meta> meta_;
    std::optional<Pager> pager_;
    std::optional<TxManager> tx_manager_;
    std::optional<Logger> logger_;
};

}