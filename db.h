#pragma once

#include "noncopyable.h"
#include "tx.h"

namespace yudb {

class DB : noncopyable {
public:
    DB() = default;
    virtual ~DB();

    static std::unique_ptr<DB> Open(std::string_view path);
    virtual UpdateTx Update() = 0;
    virtual ViewTx View() = 0;
};

} // namespace yudb