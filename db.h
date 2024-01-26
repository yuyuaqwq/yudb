#pragma once

#include <string>
#include <optional>
#include <memory>

#include "noncopyable.h"
#include "file.h"
#include "meta_operator.h"
#include "pager.h"
#include "txer.h"

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
        return txer_.Update();
    }

    ViewTx View() {
        return txer_.View();
    }

public:
    friend class Metaer;
    friend class Pager;
    friend class Txer;

    File file_;
    MetaOperator meta_operator_{ this };
    std::optional<Pager> pager_;
    Txer txer_{ this };
};

}