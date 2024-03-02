#pragma once

#include "util\noncopyable.h"
#include "db\options.h"
#include "db\tx.h"

namespace yudb {

class DB : noncopyable {
public:
    DB() = default;
    virtual ~DB();

    static std::unique_ptr<DB> Open(const Options& options, std::string_view path);
    virtual UpdateTx Update() = 0;
    virtual ViewTx View() = 0;
};

} // namespace yudb