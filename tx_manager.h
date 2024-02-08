#pragma once

#include <optional>
#include <map>

#include "noncopyable.h"
#include "tx.h"


namespace yudb {

class DBImpl;

class TxManager : noncopyable {
public:
    TxManager(DBImpl* db);
    ~TxManager() {
        if (!view_tx_map_.empty()) {
            throw std::runtime_error("There are read transactions that have not been exited.");
        }
        if (update_tx_.has_value()) {
            throw std::runtime_error("There are write transactions that have not been exited.");
        }
    }

    const Pager& pager() const;
    Pager& pager();

    UpdateTx Update();
    ViewTx View();

    void RollBack();
    void RollBack(TxId view_txid);
    void Commit();

    TxImpl& CurrentUpdateTx() { return *update_tx_; }

private:
    DBImpl* const db_;
    std::optional<TxImpl> update_tx_;
    std::map<TxId, uint32_t> view_tx_map_;
};

} // namespace yudb