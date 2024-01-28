#pragma once

#include <cstdint>

namespace yudb {

using TxId = uint64_t;
constexpr TxId kInvalidTxId = 0xffffffffffffffff;

} // namespace yudb