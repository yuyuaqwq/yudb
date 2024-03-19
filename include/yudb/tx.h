#pragma once

#include <vector>
#include <shared_mutex>

#include "yudb/tx_impl.h"
#include "yudb/bucket.h"

namespace yudb {

class ViewTx : noncopyable {
public:
    ViewTx(TxManager* tx_manager, const MetaStruct& meta, std::shared_mutex* mmap_mutex);
    ~ViewTx();

    ViewBucket UserBucket();

private:
    friend class TxManager;

    std::shared_lock<std::shared_mutex> mmap_lock_;
    TxImpl tx_;
};

class UpdateTx : noncopyable {
public:
    explicit UpdateTx(TxImpl* tx);
    ~UpdateTx();

    UpdateTx(UpdateTx&& right) noexcept;

    UpdateBucket UserBucket();
    void RollBack();
    void Commit();

private:
    TxImpl* tx_;
};

} // namespace yudb