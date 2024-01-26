#pragma once
#include <vector>

#include "tx.h"

namespace yudb {

class ViewTx {
public:
    ViewTx(TxManager* tx_manager, const Meta& meta) : tx_{ tx_manager, meta, false } {}

    ~ViewTx() {
        tx_.RollBack(tx_.txid());
    }

public:
    ViewBucket RootBucket() {
        auto& root_bucket = tx_.RootBucket();
        return ViewBucket{ &root_bucket.SubBucket("user", false) };
    }

private:
    Tx tx_;
};

class UpdateTx {
public:
    UpdateTx(Tx* tx) : tx_{ tx } {}

    ~UpdateTx() {
        if (tx_) {
            RollBack();
        }
    }

public:
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
    Tx* tx_;
};

} // namespace yudb