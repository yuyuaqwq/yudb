#pragma once

#include <cassert>

#include <span>

#include "db/log_format.h"
#include "util/noncopyable.h"
#include "util/crc32.h"
#include "util/file.h"

namespace yudb {
namespace log {

class Writer : noncopyable {
public:
    Writer() : 
        block_offset_{ 0 },
        size_{ 0 } {}
    ~Writer() = default;

    void Open(std::string_view path) {
        if (!file_.Open(path, true)) {
            throw std::runtime_error("failed to open log file.");
        }
        file_.Seek(0, File::PointerMode::kDbFilePointerSet);
        buffer_.resize(kBlockSize);
    }
    void AppendRecordToBuffer(std::span<const uint8_t> data) {
        if (kBlockSize - size_ < kHeaderSize + data.size()) {
            FlushBuffer();
            if (kHeaderSize + data.size() > kBlockSize) {
                AppendRecord(data);
                return;
            }
        }
        uint8_t buf[kHeaderSize];
        auto record = reinterpret_cast<LogRecord*>(buf);
        record->type = RecordType::kFullType;
        record->size = data.size();
        Crc32 crc32;
        crc32.Append(&record->size, kHeaderSize - sizeof(record->checksum));
        crc32.Append(data.data(), data.size());
        record->checksum = crc32.End();
        std::memcpy(&buffer_[size_], buf, kHeaderSize);
        size_ += kHeaderSize;
        std::memcpy(&buffer_[size_], data.data(), data.size());
        size_ += data.size();
    }
    void AppendRecordToBuffer(std::string_view str) {
        AppendRecordToBuffer(std::span{ reinterpret_cast<const uint8_t*>(str.data()), str.size() });
    }
    void FlushBuffer() {
        if (size_ > 0) {
            file_.Write(buffer_.data(), buffer_.size());
            size_ = 0;
        }
    }
    void AppendRecord(std::span<const uint8_t> data) {
        auto ptr = data.data();
        auto left = data.size();
        bool begin = true;
        do {
            const size_t leftover = kBlockSize - block_offset_;
            if (leftover < kHeaderSize) {
                if (leftover > 0) {
                    file_.Seek(0, File::PointerMode::kDbFilePointerEnd);
                    file_.Write(kBlockPadding, leftover);
                }
                block_offset_ = 0;
            }

            const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
            const size_t fragment_length = (left < avail) ? left : avail;

            RecordType type;
            const bool end = (left == fragment_length);
            if (begin && end) {
                type = RecordType::kFullType;
            } else if (begin) {
                type = RecordType::kFirstType;
            } else if (end) {
                type = RecordType::kLastType;
            } else {
                type = RecordType::kMiddleType;
            }

            EmitPhysicalRecord(type, ptr, fragment_length);
            ptr += fragment_length;
            left -= fragment_length;
            begin = false;
        } while (left > 0);
    }
    void AppendRecord(std::string_view str) {
        AppendRecord(std::span<const uint8_t>{ reinterpret_cast<const uint8_t*>(str.data()), str.size() });
    }

private:
    void EmitPhysicalRecord(RecordType type, const uint8_t* ptr, size_t size) {
        assert(size < 0xffff);
        assert(block_offset_ + kHeaderSize + size <= kBlockSize);

        uint8_t buf[kHeaderSize];
        auto record = reinterpret_cast<LogRecord*>(buf);
        record->type = type;
        record->size = size;

        Crc32 crc32;
        crc32.Append(&record->size, kHeaderSize - sizeof(record->checksum));
        crc32.Append(ptr, size);
        record->checksum = crc32.End();

        file_.Write(buf, kHeaderSize);
        file_.Write(ptr, size);

        block_offset_ += kHeaderSize + size;
    }

private:
    File file_;
    size_t block_offset_;
    std::vector<uint8_t> buffer_;
    size_t size_;
};

} // namespace log
} // namespace yudb