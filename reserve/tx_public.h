#pragma once
#include "tx.h"

namespace yudb {

class ViewTx {
public:
    ViewTx(Txer* txer, const Meta& meta) : tx_{ txer, meta, false } {}

public:
    ViewBucket RootBucket() {
        return ViewBucket{ &tx_.RootBucket()};
    }

private:
    Tx tx_;
};

class UpdateTx {
public:
    UpdateTx(Tx* tx) : tx_{ tx } {}

public:
    UpdateBucket RootBucket() {
        return UpdateBucket{ &tx_->RootBucket() };
    }

    void RollBack() {}

    void Commit() {
        tx_->Commit();
    }

private:
    Tx* tx_;
};

} // namespace yudb