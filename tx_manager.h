#pragma once

#include <optional>
#include <map>

#include "noncopyable.h"
#include "tx.h"
#include "log_writer.h"

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

    TxImpl& CurrentUpdateTx();

    void AppendPutLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value);
    void AppendInsertLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value);
    void AppendDeleteLog(BucketId bucket_id, std::span<const uint8_t> key);

    Pager& pager();

private:
    template<typename Iter> void AppendLog(const Iter begin, const Iter end);
    void AppendBeginLog();
    void AppendRollbackLog();
    void AppendCommitLog();

private:
    DBImpl* const db_;
    std::optional<TxImpl> update_tx_;
    std::map<TxId, uint32_t> view_tx_map_;

};

} // namespace yudb