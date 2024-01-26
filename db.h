#pragma once

#include <string>
#include <optional>
#include <memory>

#include "noncopyable.h"
#include "file.h"
#include "meta_operator.h"
#include "pager.h"
#include "tx_manager.h"

namespace yudb {

class Db : noncopyable {
public:
    Db() = default;

    static std::unique_ptr<Db> Open(std::string_view path) {
        auto db = std::make_unique<Db>();
        if (!db->file_.Open(path, false)) {
            return {};
        }
        if (!db->meta_operator_.Load()) {
            return {};
        }
        db->pager_ = Pager{ db.get(), db->meta_operator_.meta().page_size };
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
    MetaOperator meta_operator_{ this };
    std::optional<Pager> pager_;
    TxManager tx_manager_{ this };
};

}