#pragma once

#include <cstdint>

#include "db/tx_format.h"

namespace yudb {

enum class TxLogType : uint8_t {
    kBegin,
    kRollback,
    kCommit,
    kBucketInsert,
    kBucketPut,
    kBucketDelete,
};

#pragma pack(push, 1)
struct TxLogBucketFormat {
    TxLogType type;
    BucketId bucket_id;
};
#pragma pack(pop)

} // namespace yudb