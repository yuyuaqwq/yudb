#pragma once

#include <string>
#include <optional>

#include "file.h"
#include "metaer.h"
#include "pager.h"

namespace yudb {

class Db {
public:
    Db() = default;
    Db(const Db&) = delete;
    void operator=(const Db&) = delete;

    static std::optional<Db> Open(std::string_view path) {
        Db db;
        if (!db.file_.Open(path, false)) {
            return {};
        }

        if (!db.metaer_.Load()) {
            return {};
        }


    }

private:
    friend class Metaer;

    File file_;
    Metaer metaer_{ this };
    Pager pager_{ this };
};

}