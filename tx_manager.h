#pragma once

#include <memory>
#include <map>

#include "noncopyable.h"
#include "tx.h"
#include "tx_public.h"


namespace yudb {

class Db;

class TxManager : noncopyable {
public:
    TxManager(Db* db);


    UpdateTx Update();

    ViewTx View();


    void RollBack();

    void RollBack(TxId txid);

    void Commit();


    Tx& CurrentUpdateTx() { return *update_tx_; }

    Pager& pager();

public:
    static void CopyMeta(Meta* dst, const Meta& src);

private:
    Db* db_;
    std::unique_ptr<Tx> update_tx_;
    std::map<TxId, uint32_t> view_tx_map_;
};

} // namespace yudb