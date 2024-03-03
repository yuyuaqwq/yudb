#pragma once
#include <vector>

#include "yudb/tx_impl.h"
#include "yudb/bucket.h"

namespace yudb {

class ViewTx {
public:
    ViewTx(TxManager* tx_manager, const MetaStruct& meta);
    ~ViewTx();

    ViewBucket RootBucket();

private:
    TxImpl tx_;
};

class UpdateTx {
public:
    explicit UpdateTx(TxImpl* tx);
    ~UpdateTx();

    UpdateBucket UserBucket();
    void RollBack();
    void Commit();

private:
    TxImpl* tx_;
};

} // namespace yudb