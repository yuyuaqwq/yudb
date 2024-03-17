#pragma once

#include <cstdint>

#include "yudb/tx_format.h"

namespace yudb {

enum class LogType : uint8_t {
    kPersisted,
    kBegin,
    kRollback,
    kCommit,
    kSubBucket,
    kPut_IsBucket,
    kPut_NotBucket,
    kDelete,
};

#pragma pack(push, 1)
struct PersistedLogHeader {
    LogType type;
    TxId txid;
};

struct BucketLogHeader {
    LogType type;
    BucketId bucket_id;
};
#pragma pack(pop)

constexpr size_t kBucketSubBucketLogHeaderSize = sizeof(BucketLogHeader);
constexpr size_t kBucketPutLogHeaderSize = sizeof(BucketLogHeader);
constexpr size_t kBucketUpdateLogHeaderSize = sizeof(BucketLogHeader);
constexpr size_t kBucketDeleteLogHeaderSize = sizeof(BucketLogHeader);

} // namespace yudb