#pragma once

#include <cstdint>

namespace yudb {

using TxId = uint64_t;
constexpr TxId kTxInvalidId = 0xffffffffffffffff;

using BucketId = uint32_t;
constexpr BucketId kUserBucketId = 0xffffffff;

} // namespace yudb