#pragma once

#include <cstdint>
#include <memory>
#include <string_view>

#include "noncopyable.h"
#include "tx_id.h"
#include "meta.h"
#include "bucket.h"


namespace yudb {

class Txer;

class Tx : noncopyable {
public:
    Tx(Txer* txer, const Meta& meta) :
        txer_{ txer },
        meta_{ meta } {}

    Tx(Tx&& right) noexcept :
        txer_{ right.txer_ },
        meta_{ std::move(right.meta_) } {}

    void operator=(Tx&& right) noexcept {
        txer_ = right.txer_;
        meta_ = std::move(right.meta_);
    }

    TxId tx_id() { return meta_.tx_id; }
    
protected:
    Pager* pager();

protected:
    friend class Txer;

    Txer* txer_;
    Meta meta_;
};

class ViewTx : public Tx {
public:
    ViewTx(Txer* txer, const Meta& meta) :
        Tx{ txer, meta },
        bucket_{ pager(), this, meta_.root} {}

public:
    ViewBucket& RootBucket();

private:
    ViewBucket bucket_;
};


class UpdateTx : public Tx {
public:
    UpdateTx(Txer* txer, const Meta& meta) :
        Tx{ txer, meta },
        bucket_{ pager(), this, meta_.root } {}

public:
    UpdateBucket& RootBucket();


    void RollBack() {

    }

    void Commit() {

    }

private:
    UpdateBucket bucket_;
};

} // namespace yudb