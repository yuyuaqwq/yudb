#pragma once

#include <string>
#include <optional>

#include "file.h"
#include "meta_info.h"

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

        if (!db.mate_infor_.Load()) {
            return {};
        }


    }

private:
    friend class MetaInfor;

    File file_;
    MetaInfor mate_infor_;
    
};

}