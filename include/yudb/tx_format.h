#pragma once

#include <cstdint>

namespace yudb {

using TxId = uint64_t;
constexpr TxId kTxInvalidId = 0xffffffffffffffff;

using BucketId = uint32_t;
constexpr BucketId kRootBucketId = 0xffffffff;
constexpr BucketId kFreeBucketId = 0xfffffffe;

} // namespace yudb