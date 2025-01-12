//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <string>
#include <optional>
#include <memory>
#include <shared_mutex>

#include <mio/mio.hpp>

#include <yudb/db.h>

#include "meta.h"
#include "shm.h"
#include "tx_manager.h"
#include "pager.h"
#include "logger.h"

namespace yudb {

class DBImpl : public DB {
public:
    DBImpl() = default;
    ~DBImpl() override;

    UpdateTx Update() override;
    ViewTx View() override;

    void Remmap(uint64_t new_size);
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
    auto& logger() const { return *logger_; }
    auto& logger() { return *logger_; }

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