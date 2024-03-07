#pragma once

#include <string>
#include <optional>
#include <memory>
#include <shared_mutex>

#include "third_party/mio.hpp"
#include "yudb/db.h"
#include "yudb/log_writer.h"
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
    void Checkpoint();

    void Mmap(uint64_t new_size);
    void ClearMmap();

    template<typename Iter>
    void AppendLog(const Iter begin, const Iter end) {
        if (recovering_) return;
        for (auto it = begin; it != end; ++it) {
            log_writer_.AppendRecordToBuffer(*it);
        }
        if (log_writer_.size() >= options_->checkpoint_wal_threshold && tx_manager_.committing()) {
            Checkpoint();
        }
    }

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
    auto& meta() const { return meta_; }
    auto& meta() { return meta_; }
    auto& pager() const { assert(pager_.has_value()); return *pager_; }
    auto& pager() { assert(pager_.has_value()); return *pager_; }
    auto& tx_manager() const { return tx_manager_; }
    auto& tx_manager() { return tx_manager_; }
    auto& log_writer() const { return log_writer_; }
    auto& log_writer() { return log_writer_; }

private:
    void Recover(std::string_view log_path);
    void AppendInitLog();
    
private:
    friend class DB;

    std::optional<Options> options_;
    bool recovering_{ false };

    std::string db_path_;
    tinyio::file db_file_;
    mio::mmap_sink db_mmap_;
    std::shared_mutex db_mmap_lock_;
    std::vector<mio::mmap_sink> db_mmap_pending_;

    mio::mmap_sink shm_mmap_;
    std::optional<Shm> shm_;

    Meta meta_{ this };
    std::optional<Pager> pager_;
    TxManager tx_manager_{ this };
    log::Writer log_writer_;
};

}