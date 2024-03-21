#pragma once

#include <atomic>
#include <optional>
#include <map>

#include "pool/static_memory_pool.hpp"
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

    // 读事务回滚
    void RollBack(TxId view_txid);
    // 写事务回滚
    void RollBack();
    // 写事务提交
    void Commit();

    bool IsTxExpired(TxId view_txid) const;

    void AppendSubBucketLog(BucketId bucket_id, std::span<const uint8_t> key);
    void AppendPutLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket);
    void AppendDeleteLog(BucketId bucket_id, std::span<const uint8_t> key);

    DBImpl& db();
    Pager& pager() const;
    bool has_update_tx() const { return update_tx_.has_value(); };
    TxImpl& update_tx();
    TxImpl& view_tx(ViewTx* view_tx);
    auto& persisted_txid() const { return persisted_txid_; }
    void set_persisted_txid(TxId new_persisted_txid) { persisted_txid_ = new_persisted_txid; }

private:
    void AppendBeginLog();
    void AppendRollbackLog();
    void AppendCommitLog();

private:
    DBImpl* const db_;

    TxId persisted_txid_{ kTxInvalidId };
    std::optional<TxImpl> update_tx_;

    std::map<TxId, uint32_t, std::less<>,
        pool::StaticMemoryPool<std::pair<const TxId, uint32_t>>> view_tx_map_;      // txid : view_tx_count
    TxId min_view_txid_;        // 仅在写事务更新
};

} // namespace yudb