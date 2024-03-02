#pragma once

#include <cassert>

#include <optional>
#include <string>
#include <ios>

#include <Windows.h>
#undef min

#include "util/noncopyable.h"

namespace yudb {

class File : noncopyable {
public:
    enum class PointerMode {
        kDbFilePointerSet = FILE_BEGIN,
        kDbFilePointerCur = FILE_CURRENT,
        kDbFilePointerEnd = FILE_END,
    };

public:
    File();
    ~File();

    File(File&& right) noexcept;
    void operator=(File&& right) noexcept;

    bool Open(std::string_view path, bool use_system_buf);
    void Close();
    void Seek(int64_t offset, PointerMode fromwhere);
    int64_t Tell();
    size_t Read(void* buf, size_t size);
    void Write(const void* buf, size_t size);
    void Sync();

private:
    HANDLE handle_{ INVALID_HANDLE_VALUE };
};

} // namespace yudb