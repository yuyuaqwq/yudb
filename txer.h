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

    void Commit();


    Pager* pager();

    UpdateTx* update_tx() { return update_tx_.get(); }


    void CopyMeta(Meta* dst, const Meta& src);

private:
    Db* db_;
    std::unique_ptr<UpdateTx> update_tx_;
};

} // namespace yudb