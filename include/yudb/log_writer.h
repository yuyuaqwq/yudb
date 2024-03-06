#pragma once

#include <cassert>

#include <span>
#include <filesystem>

#include "third_party/tinyio.hpp"
#include "yudb/log_format.h"
#include "yudb/noncopyable.h"

namespace yudb {
namespace log {

class Writer : noncopyable {
public:
    Writer();
    ~Writer();

    void Open(std::string_view path);
    void Close();
    void Reset();
    void AppendRecordToBuffer(std::span<const uint8_t> data);
    void AppendRecordToBuffer(std::string_view data);
    void WriteBuffer();

    std::string_view path() { return path_; }
    size_t size() { return size_; }

private:
    void AppendRecord(std::span<const uint8_t> data);
    void EmitPhysicalRecord(RecordType type, const uint8_t* ptr, size_t size);

private:
    tinyio::file file_;
    std::string path_;
    size_t size_{ 0 };
    size_t block_offset_{ 0 };
    std::vector<uint8_t> rep_;
};

} // namespace log
} // namespace yudb