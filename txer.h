#pragma once

#include <memory>

#include "noncopyable.h"
#include "tx.h"

namespace yudb {

class Db;

class Txer : noncopyable {
public:
    Txer(Db* db);

    UpdateTx& Update();

    ViewTx View();

private:
    void CopyMeta(Meta* dst, const Meta& src);

    void Commit();

private:
    friend class Tx;
    friend class ViewTx;
    friend class UpdateTx;

    Db* db_;
    std::unique_ptr<UpdateTx> update_tx_;
};

} // namespace yudb