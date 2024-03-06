#pragma once

#include <optional>
#include <map>
#include <mutex>

#include "yudb/noncopyable.h"
#include "yudb/log_writer.h"
#include "yudb/tx.h"

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

    void AppendPutLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value);
    void AppendInsertLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value);
    void AppendDeleteLog(BucketId bucket_id, std::span<const uint8_t> key);

    DBImpl& db();
    TxImpl& update_tx();
    bool has_update_tx() { return update_tx_.has_value(); };
    Pager& pager() const;
    bool committed() const { return committed_; }

private:
    void AppendBeginLog();
    void AppendRollbackLog();
    void AppendCommitLog();

private:
    DBImpl* const db_;
    bool first_{ true };
    std::optional<TxImpl> update_tx_;
    std::map<TxId, uint32_t> view_tx_map_;
    bool committed_{ false };

    std::mutex update_lock_;
};

} // namespace yudb