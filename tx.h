#pragma once

#include <cstdint>
#include <memory>
#include <string_view>

#include "noncopyable.h"
#include "txid.h"
#include "meta.h"
#include "bucket.h"


namespace yudb {

class Txer;

class Tx : noncopyable {
public:
    Tx(Txer* txer, const Meta& meta);

    Tx(Tx&& right) noexcept;
    void operator=(Tx&& right) noexcept;

    TxId txid() { return meta_.txid; }

protected:
    Pager* pager();

    Meta& meta() { return meta_; }

protected:
    friend class Txer;

    Txer* txer_;
    Meta meta_;
};

class ViewTx : public Tx {
public:
    ViewTx(Txer* txer, const Meta& meta);

public:
    ViewBucket& RootBucket();

private:
    friend class Txer;

    ViewBucket bucket_;
};


class UpdateTx : public Tx {
public:
    UpdateTx(Txer* txer, const Meta& meta);

public:
    UpdateBucket& RootBucket();

    void RollBack();

    void Commit();

    bool IsExpiredTxId(TxId txid) {
        return txid < this->txid();
    }

private:
    friend class Txer;
    friend class Pager;

    UpdateBucket bucket_;
};

} // namespace yudb