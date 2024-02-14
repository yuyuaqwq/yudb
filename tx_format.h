#pragma once

#include <cstdint>

namespace yudb {

using TxId = uint64_t;
constexpr TxId kInvalidTxId = 0xffffffffffffffff;

using BucketId = uint32_t;
constexpr BucketId kRootBucketId = 0xffffffff;

} // namespace yudb