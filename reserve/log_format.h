#pragma once

#include <cstdint>

namespace yudb {

namespace log {

enum class RecordType : uint8_t {
    kZeroType = 0,
    kFullType = 1,
    kFirstType = 2,
    kMiddleType = 3,
    kLastType = 4
};

#pragma pack(push, 1)
struct LogRecord {
    uint32_t checksum;
    uint16_t size;
    RecordType type;
    uint8_t data[];
};
#pragma pack(pop)

static constexpr size_t kBlockSize = 32 * 1024;
static constexpr size_t kHeaderSize = sizeof(LogRecord);
static const char* kBlockPadding = "\x00\x00\x00\x00\x00\x00";

} // namespace log

} // namespace yudb