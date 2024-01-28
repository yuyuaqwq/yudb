#pragma once

#include <string>
#include <optional>
#include <memory>

#include "noncopyable.h"
#include "file.h"
#include "meta.h"
#include "pager.h"
#include "tx_manager.h"
#include "log_writer.h"

namespace yudb {

class DB : noncopyable {
public:
    DB() = default;

    static std::unique_ptr<DB> Open(std::string_view path) {
        auto db = std::make_unique<DB>();
        if (!db->file_.Open(path, false)) {
            return {};
        }
        if (!db->meta_.Load()) {
            return {};
        }
        db->pager_ = Pager{ db.get(), db->meta_.meta_format().page_size };
        return db;
    }

    UpdateTx Update() {
        return tx_manager_.Update();
    }

    ViewTx View() {
        return tx_manager_.View();
    }

public:
    friend class MetaManager;
    friend class Pager;
    friend class TxManager;

    File file_;
    Meta meta_{ this };
    std::optional<Pager> pager_;
    TxManager tx_manager_{ this };
    //log::Writer log_writer_;
};

}