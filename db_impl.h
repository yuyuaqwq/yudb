#pragma once

#include <string>
#include <optional>
#include <memory>

#include "db.h"
#include "file.h"
#include "meta.h"
#include "pager.h"
#include "tx_manager.h"

namespace yudb {

class DBImpl : public DB {
public:
    DBImpl() = default;
    ~DBImpl() override;

    UpdateTx Update() override;
    ViewTx View() override;

    void BuildPager(DBImpl* db, PageSize page_size);

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
    File file_;
    Meta meta_{ this };
    std::optional<Pager> pager_;
    TxManager tx_manager_{ this };

    log::Writer log_writer_;
};

}