#pragma once
#include <vector>

#include "db/tx_impl.h"

namespace yudb {

class ViewTx {
public:
    ViewTx(TxManager* tx_manager, const MetaStruct& meta) : tx_{ tx_manager, meta, false } {}
    ~ViewTx() {
        tx_.RollBack();
    }

    ViewBucket RootBucket() {
        auto& root_bucket = tx_.root_bucket();
        return ViewBucket{ &root_bucket.SubBucket(kUserDBKey, false) };
    }

private:
    TxImpl tx_;
};

class UpdateTx {
public:
    UpdateTx(TxImpl* tx) : tx_{ tx } {}
    ~UpdateTx() {
        if (tx_) {
            RollBack();
        }
    }

    UpdateBucket UserBucket() {
        auto& root_bucket = tx_->root_bucket();
        return UpdateBucket{ &root_bucket.SubBucket(kUserDBKey, true) };
    }
    void RollBack() {
        assert(tx_ != nullptr);
        tx_->RollBack();
        tx_ = nullptr;
    }
    void Commit() {
        tx_->Commit();
        tx_ = nullptr;
    }

private:
    TxImpl* tx_;
};

} // namespace yudb