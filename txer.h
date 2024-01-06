#pragma once

#include <memory>

#include "noncopyable.h"
#include "tx.h"
#include "tx_public.h"


namespace yudb {

class Db;

class Txer : noncopyable {
public:
    Txer(Db* db);


    UpdateTx Update();

    ViewTx View();

    void Commit();


    Tx& CurrentUpdateTx() { return *update_tx_; }



    Pager& pager();

public:
    static void CopyMeta(Meta* dst, const Meta& src);

private:
    Db* db_;
    std::unique_ptr<Tx> update_tx_;
};

} // namespace yudb