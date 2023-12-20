#pragma once

#include <cstdint>
#include <memory>
#include <string_view>

#include "tx_id.h"
#include "meta.h"
#include "bucket.h"


namespace yudb {

class Txer;
class Bucket;

class Tx {
public:
    Tx(Txer* txer, const Meta& meta);
    
    Bucket Bucket(std::string_view key);

    TxId tx_id() { return meta_.tx_id; }

    void RollBack() {

    }

    void Commit() {

    }

private:
    friend class Txer;
    friend class Bucket;

    Txer* txer_;
    Meta meta_;
};

} // namespace yudb