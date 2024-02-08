#pragma once
#include <vector>

#include "tx_impl.h"

namespace yudb {

class ViewTx {
public:
    ViewTx(TxManager* tx_manager, const MetaFormat& meta) : tx_{ tx_manager, meta, false } {}
    ~ViewTx() {
        tx_.RollBack();
    }

    ViewBucket RootBucket() {
        auto& root_bucket = tx_.RootBucket();
        return ViewBucket{ &root_bucket.SubBucket("user", false) };
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

    UpdateBucket RootBucket() {
        auto& root_bucket = tx_->RootBucket();
        return UpdateBucket{ &root_bucket.SubBucket("user", true) };
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