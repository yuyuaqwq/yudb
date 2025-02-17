//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <wal/log_writer.h>

#include <atomkv/noncopyable.h>

#include "log_type.h"

namespace atomkv {

class DBImpl;

class Logger : noncopyable {
public:
    Logger(DBImpl* db, std::string_view log_path);
    ~Logger();

    void AppendLog(const std::span<const uint8_t>* begin, const std::span<const uint8_t>* end);
    void AppendWalTxIdLog();
    void FlushLog();

    void Reset();
    bool CheckPointNeeded() const { return checkpoint_needed_; }
    void Checkpoint();
    bool RecoverNeeded();
    void Recover();


private:
    DBImpl* const db_;

    const std::string log_path_;
    wal::Writer writer_;
    bool disable_writing_{ false };

    bool checkpoint_needed_{ false };
};

} // namespace atomkv