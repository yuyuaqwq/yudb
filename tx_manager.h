#pragma once

#include <optional>
#include <map>

#include "noncopyable.h"
#include "tx.h"

namespace yudb {

class DBImpl;

class TxManager : noncopyable {
public:
    explicit TxManager(DBImpl* db);
    ~TxManager();

    UpdateTx Update();
    ViewTx View();

    void RollBack();
    void RollBack(TxId view_txid);
    void Commit();

    TxImpl& CurrentUpdateTx() { return *update_tx_; }

    const Pager& pager() const;
    Pager& pager();

private:
    DBImpl* const db_;
    std::optional<TxImpl> update_tx_;
    std::map<TxId, uint32_t> view_tx_map_;
};

} // namespace yudb