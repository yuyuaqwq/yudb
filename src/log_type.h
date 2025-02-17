//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <cstdint>

#include "atomkv/tx_format.h"

namespace atomkv {

enum class LogType : uint8_t {
    kWalTxId,
    kBegin,
    kRollback,
    kCommit,
    kSubBucket,
    kPut_IsBucket,
    kPut_NotBucket,
    kDelete,
};

#pragma pack(push, 1)
struct WalTxIdLogHeader {
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

} // namespace atomkv