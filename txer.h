#pragma once

#include "noncopyable.h"
#include "tx.h"

namespace yudb {

class Db;

class Txer : noncopyable {
public:
    Txer(Db* db) : db_{ db } {}

    UpdateTx Update();

    ViewTx View();

private:
    friend class Tx;

    Db* db_;
};

} // namespace yudb