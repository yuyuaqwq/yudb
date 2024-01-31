#pragma once

#include <string>
#include <optional>
#include <memory>

#include "db.h"
#include "file.h"
#include "meta.h"
#include "pager.h"
#include "tx_manager.h"
#include "log_writer.h"

namespace yudb {

class DBImpl : public DB {
public:
    DBImpl() = default;
    ~DBImpl() override {
        if (pager_.has_value()) {
            //pager_->SyncAllPage();
        }
    }

    const auto& file() const { return file_; }
    auto& file() { return file_; }
    const auto& meta() const { return meta_; }
    auto& meta() { return meta_; }
    const auto& pager() const { assert(pager_.has_value()); return *pager_; }
    auto& pager() { assert(pager_.has_value()); return *pager_; }
    void set_pager(Pager&& pager) { pager_ = std::move(pager); pager_->set_db(this); }
    const auto& tx_manager() const { return tx_manager_; }
    auto& tx_manager() { return tx_manager_; }
    const auto& log_writer() const { return log_writer_; }
    auto& log_writer() { return log_writer_; }

    UpdateTx Update() override {
        return tx_manager_.Update();
    }
    ViewTx View() override {
        return tx_manager_.View();
    }

private:
    File file_;
    Meta meta_{ this };
    std::optional<Pager> pager_;
    TxManager tx_manager_{ this };
    log::Writer log_writer_;
};

}