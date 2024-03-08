#pragma once

#include "yudb/log_writer.h"
#include "yudb/log_type.h"
#include "yudb/noncopyable.h"

namespace yudb {

class DBImpl;

class Logger : noncopyable {
public:
    Logger(DBImpl* db, std::string_view log_path);
    ~Logger();

    void AppendLog(const std::span<const uint8_t>* begin, const std::span<const uint8_t>* end);
    void AppendPersistedLog();
    void FlushLog();

    void Reset();
    void Checkpoint();
    void Recover();

private:
    DBImpl* const db_;

    std::string log_path_;
    log::Writer writer_;
    bool recovering_{ false };
};

} // namespace yudb