//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

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
                throw LoggerError{ "partial record without end(1)." };
            }
            return std::string{ reinterpret_cast<const char*>(record->data), record->size };
        }
        case RecordType::kFirstType: {
            if (in_fragmented_record) {
                throw LoggerError{ "partial record without end(2)." };
            }
            in_fragmented_record = true;
            res.append(reinterpret_cast<const char*>(record->data), record->size);
            break;
        }
        case RecordType::kMiddleType: {
            if (!in_fragmented_record) {
                throw LoggerError{ "missing start of fragmented record(1)." };
            }
            res.append(reinterpret_cast<const char*>(record->data), record->size);
            break;
        }
        case RecordType::kLastType: {
            if (!in_fragmented_record) {
                throw LoggerError{ "missing start of fragmented record(2)." };
            }
            res.append(reinterpret_cast<const char*>(record->data), record->size);
            in_fragmented_record = false;
            return res;
        }
        default: {
            throw LoggerError{ "unknown record type." };
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
                    // Reached the end of the file
                    eof_ = true;
                    return nullptr;
                }
                else if (size_ < kBlockSize) {
                    // Reached the end of the file, but still some data remains, continue processing
                    eof_ = true;
                }
                continue;
            }
            else {
                // Possibly crashed when writing the header
                size_ = 0;
                return nullptr;
            }
        }

        record = reinterpret_cast<const LogRecord*>(&buffer_[offset_]);
        if (kHeaderSize + record->size > size_) {
            size_ = 0;
            if (!eof_) {
                throw LoggerError{ "incorrect log record size." };
            }
            // Possibly crashed when writing data
            return nullptr;
        }

        if (record->type == RecordType::kZeroType && record->size == 0) {
            // Scenario where the block exactly leaves 7 bytes when writing, size must be 0
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