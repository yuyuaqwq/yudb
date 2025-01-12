//The MIT License(MIT)
//Copyright ?? 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

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

    // Read transaction rollback
    void RollBack(TxId view_txid);
    // Write transaction rollback
    void RollBack();
    // Write transaction commit
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
    TxId min_view_txid_;        // Only updated in write transactions

};

} // namespace yudb