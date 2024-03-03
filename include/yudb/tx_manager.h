#pragma once

#include <optional>
#include <map>

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

    TxImpl& update_tx();
    bool has_update_tx() { return update_tx_.has_value(); };

    void AppendPutLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value);
    void AppendInsertLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value);
    void AppendDeleteLog(BucketId bucket_id, std::span<const uint8_t> key);

    Pager& pager() const;

private:
    void AppendBeginLog();
    void AppendRollbackLog();
    void AppendCommitLog();

private:
    DBImpl* const db_;
    bool first_{ true };
    std::optional<TxImpl> update_tx_;
    std::map<TxId, uint32_t> view_tx_map_;

};

} // namespace yudb