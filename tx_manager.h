#pragma once

#include <memory>
#include <map>

#include "noncopyable.h"
#include "tx.h"
#include "tx_public.h"


namespace yudb {

class DB;

class TxManager : noncopyable {
public:
    TxManager(DB* db);


    UpdateTx Update();

    ViewTx View();


    void RollBack();

    void RollBack(TxId txid);

    void Commit();


    Tx& CurrentUpdateTx() { return *update_tx_; }

    Pager& pager();

public:
    static void CopyMeta(MetaFormat* dst, const MetaFormat& src);

private:
    DB* db_;
    std::unique_ptr<Tx> update_tx_;
    std::map<TxId, uint32_t> view_tx_map_;
};

} // namespace yudb