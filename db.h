#pragma once

#include <string>
#include <optional>
#include <memory>

#include "noncopyable.h"
#include "file.h"
#include "metaer.h"
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

        if (!db->metaer_.Load()) {
            return {};
        }

        db->pager_ = std::make_unique<Pager>(db.get(), db->metaer_.meta().page_size);
        db->pager_->set_page_count(db->metaer_.meta().page_count);

        return db;
    }

    Tx Begin() {
        return Tx{ &txer_, metaer_.meta().root };
    }

public:
    friend class Metaer;
    friend class Pager;
    friend class Txer;
    friend class Tx;

    File file_;
    Metaer metaer_{ this };
    std::unique_ptr<Pager> pager_;
    Txer txer_{ this };
};

}