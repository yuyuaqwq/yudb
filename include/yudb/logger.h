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
    log::Writer writer_;
    bool disable_writing_{ false };
    bool checkpoint_needed_{ false };
};

} // namespace yudb