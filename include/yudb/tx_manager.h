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

    TxImpl& Update(Comparator comparator);
    ViewTx View(Comparator comparator);

    void RollBack();
    void RollBack(TxId view_txid);
    void Commit();

    bool IsViewExists(TxId view_txid) const;

    void AppendSubBucketLog(BucketId bucket_id, std::span<const uint8_t> key);
    void AppendPutLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket);
    void AppendDeleteLog(BucketId bucket_id, std::span<const uint8_t> key);

    DBImpl& db();
    TxImpl& update_tx();
    bool has_update_tx() const { return update_tx_.has_value(); };
    Pager& pager() const;

private:
    void AppendBeginLog();
    void AppendRollbackLog();
    void AppendCommitLog();

private:
    DBImpl* const db_;

    std::optional<TxImpl> update_tx_;
    std::map<TxId, uint32_t> view_tx_map_;
};

} // namespace yudb