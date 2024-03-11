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
    bool NeedCheckPoint() const { return need_checkpoint_; }
    void Checkpoint();
    bool NeedRecover();
    void Recover();


private:
    DBImpl* const db_;

    const std::string log_path_;
    log::Writer writer_;
    bool disable_writing_{ false };
    bool need_checkpoint_{ false };
};

} // namespace yudb