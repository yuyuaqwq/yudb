#pragma once

#include <cstdint>

#include "db/tx_format.h"

namespace yudb {

enum class OperationType : uint8_t {
    kBegin,
    kRollback,
    kCommit,

    kInsert,
    kPut,
    kDelete,
};

#pragma pack(push, 1)
struct BucketLogHeader {
    OperationType type;
    BucketId bucket_id;
};

#pragma pack(pop)

constexpr size_t kBucketPutLogHeaderSize = sizeof(BucketLogHeader);
constexpr size_t kBucketDeleteLogHeaderSize = sizeof(BucketLogHeader);
constexpr size_t kBucketInsertLogHeaderSize = sizeof(BucketLogHeader);


} // namespace yudb