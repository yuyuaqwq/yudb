#pragma once

#include <vector>

#include "third_party/tinyio.hpp"
#include "yudb/log_format.h"
#include "yudb/noncopyable.h"

namespace yudb {
namespace log {

class Reader : noncopyable {
public:
    Reader();
    ~Reader();

    void Open(std::string_view path);

    std::optional<std::string> ReadRecord();

private:
    const LogRecord* ReadPhysicalRecord();

private:
    tinyio::file file_;
    std::vector<uint8_t> buffer_;
    size_t size_;
    size_t offset_;
    bool eof_;
};

} // namespace log
} // namespace yudb