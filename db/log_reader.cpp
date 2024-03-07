#include "yudb/log_reader.h"

#include "yudb/crc32.h"
#include "yudb/error.h"

namespace yudb {
namespace log {

Reader::Reader() :
    eof_{ false },
    size_{ 0 },
    offset_{ 0 } {}

Reader::~Reader() = default;

void Reader::Open(std::string_view path) {
    file_.open(path, tinyio::access_mode::write);
    buffer_.resize(kBlockSize);
    file_.seekg(0);
    buffer_.resize(kBlockSize);
}

std::optional<std::string> Reader::ReadRecord() {
    bool in_fragmented_record = false;
    std::string res;
    do {
        auto record = ReadPhysicalRecord();
        if (!record) {
            if (eof_) return {};
            else continue;
        }

        switch (record->type) {
        case RecordType::kFullType: {
            if (in_fragmented_record) {
                throw LogError{ "partial record without end(1)." };
            }
            return std::string{ reinterpret_cast<const char*>(record->data), record->size };
        }
        case RecordType::kFirstType: {
            if (in_fragmented_record) {
                throw LogError{ "partial record without end(2)." };
            }
            in_fragmented_record = true;
            res.append(reinterpret_cast<const char*>(record->data), record->size);
            break;
        }
        case RecordType::kMiddleType: {
            if (!in_fragmented_record) {
                throw LogError{ "missing start of fragmented record(1)." };
            }
            res.append(reinterpret_cast<const char*>(record->data), record->size);
            break;
        }
        case RecordType::kLastType: {
            if (!in_fragmented_record) {
                throw LogError{ "missing start of fragmented record(2)." };
            }
            res.append(reinterpret_cast<const char*>(record->data), record->size);
            in_fragmented_record = false;
            return res;
        }
        default: {
            throw LogError{ "unknown record type." };
        }
        }
    } while (true);
}

const LogRecord* Reader::ReadPhysicalRecord() {
    const LogRecord* record = nullptr;
    do {
        if (size_ < kHeaderSize) {
            if (!eof_) {
                offset_ = 0;
                size_ = file_.read(&buffer_[0], kBlockSize);
                if (size_ == 0) {
                    // 到达文件末尾
                    eof_ = true;
                    return nullptr;
                }
                else if (size_ < kBlockSize) {
                    // 到达文件末尾，但仍有数据，继续循环处理
                    eof_ = true;
                }
                continue;
            }
            else {
                // 可能是写header时crash
                size_ = 0;
                return nullptr;
            }
        }

        record = reinterpret_cast<const LogRecord*>(&buffer_[offset_]);
        if (kHeaderSize + record->size > size_) {
            size_ = 0;
            if (!eof_) {
                throw LogError{ "incorrect log record length." };
            }
            // 可能是写data时crash
            return nullptr;
        }

        if (record->type == RecordType::kZeroType && record->size == 0) {
            // 写入时block正好余下7字节的场景，size只能为0
            size_ = 0;
            return nullptr;
        }

        if (record->checksum) {
            Crc32 crc32;
            crc32.Append(&record->size, kHeaderSize - sizeof(record->checksum));
            crc32.Append(record->data, record->size);
            if (record->checksum != crc32.End()) {
                return nullptr;
            }
        }
        offset_ += kHeaderSize + record->size;
        size_ -= kHeaderSize + record->size;

        return record;
    } while (true);

    return record;
}

} // namespace log
} // namespace yudb