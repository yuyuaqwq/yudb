//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <cassert>

#include <span>
#include <vector>
#include <filesystem>

#include "tinyio/tinyio.hpp"
#include "yudb/log_format.h"
#include "yudb/noncopyable.h"

namespace yudb {
namespace log {

class Writer : noncopyable {
public:
    Writer();
    ~Writer();

    void Open(std::string_view path, tinyio::access_mode access_mode);
    void Close();
    void AppendRecordToBuffer(std::span<const uint8_t> data);
    void AppendRecordToBuffer(std::string_view data);
    void FlushBuffer();
    void Sync();

    auto& file() { return file_; }
    auto& size() const { return size_; }
   
private:
    void AppendRecord(std::span<const uint8_t> data);
    void EmitPhysicalRecord(RecordType type, const uint8_t* ptr, size_t size);

private:
    tinyio::file file_;
    size_t size_{ 0 };
    size_t block_offset_{ 0 };
    std::vector<uint8_t> rep_;
};

} // namespace log
} // namespace yudb
