#pragma once

#include "yudb\options.h"
#include "yudb\tx.h"

namespace yudb {

class DB : noncopyable {
public:
    DB() = default;
    virtual ~DB();

    DB(const DB&) = delete;
    void operator=(const DB&) = delete;

    static std::unique_ptr<DB> Open(const Options& options, std::string_view path);
    virtual UpdateTx Update() = 0;
    virtual ViewTx View() = 0;
};

} // namespace yudb