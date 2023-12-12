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

    void Put(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size) {
        std::span<const uint8_t> key_span{ reinterpret_cast<const uint8_t*>(key_buf), key_size };
        std::span<const uint8_t> value_span{ reinterpret_cast<const uint8_t*>(value_buf), value_size };
        btree_->Put(key_span, value_span);
    }

    void Put(std::string_view key, std::string_view value) {
        Put(key.data(), key.size(), value.data(), value.size());
    }

    bool Find(const void* key_buf, size_t key_size) {
        std::span<const uint8_t> key_span{ reinterpret_cast<const uint8_t*>(key_buf), key_size };
        return btree_->Find(key_span);
    }

    void Print() {
        btree_->Print();
    }

private:
    Txer* txer_;
    std::unique_ptr<BTree> btree_;
};

} // namespace yudb