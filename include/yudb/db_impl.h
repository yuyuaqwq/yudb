#pragma once

#include <string>
#include <optional>
#include <memory>

#include "third_party/mio.hpp"
#include "yudb/db.h"
#include "yudb/file.h"
#include "yudb/log_writer.h"
#include "yudb/meta.h"
#include "yudb/pager.h"
#include "yudb/tx_manager.h"

namespace yudb {

class DBImpl : public DB {
public:
    DBImpl() = default;
    ~DBImpl() override;

    UpdateTx Update() override;
    ViewTx View() override;
    void Checkpoint();

    template<typename Iter>
    void AppendLog(const Iter begin, const Iter end) {
        if (recovering_) return;
        for (auto it = begin; it != end; ++it) {
            log_writer_.AppendRecordToBuffer(*it);
        }
        if (log_writer_.size() >= options_->log_file_limit_bytes && tx_manager_.committed()) {
            Checkpoint();
        }
    }

    auto& options() const { return options_; }
    auto& options() { return options_; }
    auto& file() const { return file_; }
    auto& file() { return file_; }
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
    
private:
    friend class DB;

    std::optional<const Options> options_;
    bool recovering_{ false };

    // File file_;
    mio::mmap_sink file_;

    Meta meta_{ this };
    std::optional<Pager> pager_;
    TxManager tx_manager_{ this };
    log::Writer log_writer_;
};

}