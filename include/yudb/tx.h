#pragma once
#include <vector>

#include "yudb/tx_impl.h"
#include "yudb/bucket.h"

namespace yudb {

class ViewTx {
public:
    ViewTx(TxManager* tx_manager, const MetaStruct& meta);
    ~ViewTx();

    ViewBucket UserBucket(Comparator comparator = nullptr);

private:
    TxImpl tx_;
};

class UpdateTx {
public:
    explicit UpdateTx(TxImpl* tx);
    ~UpdateTx();

    UpdateBucket UserBucket(Comparator comparator = nullptr);
    void RollBack();
    void Commit();

private:
    TxImpl* tx_;
};

} // namespace yudb