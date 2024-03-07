#pragma once
#include <vector>

#include "yudb/tx_impl.h"
#include "yudb/bucket.h"

namespace yudb {

class ViewTx : noncopyable {
public:
    ViewTx(TxManager* tx_manager, const MetaStruct& meta);
    ~ViewTx();

    ViewBucket UserBucket(Comparator comparator = nullptr);

private:
    TxImpl tx_;
};

class UpdateTx : noncopyable {
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