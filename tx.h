#pragma once

#include <cstdint>
#include <memory>
#include <string_view>

#include "btree.h"

namespace yudb {

using TxId = uint32_t;

class Txer;

class Tx {
public:
    Tx(Txer* txer, PageId& btree_root_pgid);

    void Put(std::string_view key, std::string_view value) {
        std::span<const uint8_t> key_span{ reinterpret_cast<const uint8_t*>(key.data()), key.size() };
        std::span<const uint8_t> value_span{ reinterpret_cast<const uint8_t*>(value.data()), key.size() };
        btree_->Put(key_span, value_span);
    }

private:
    Txer* txer_;
    std::unique_ptr<BTree> btree_;
};

} // namespace yudb