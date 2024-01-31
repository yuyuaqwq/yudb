#pragma once

#include <optional>
#include <string>
#include <ios>

#include <Windows.h>

#include "noncopyable.h"

namespace yudb {

class File : noncopyable {
public:
    enum class PointerMode {
        kDbFilePointerSet = FILE_BEGIN,
        kDbFilePointerCur = FILE_CURRENT,
        kDbFilePointerEnd = FILE_END,
    };

public:
    File() = default;
    ~File() {
        Close();
    }

    File(File&& right) noexcept {
        operator=(std::move(right));
    }

    void operator=(File&& right) noexcept {
        Close();
        handle_ = right.handle_;
        right.handle_ = INVALID_HANDLE_VALUE;
    }

    bool Open(std::string_view path, bool use_system_buf) {
        assert(handle_ == INVALID_HANDLE_VALUE);
        if (use_system_buf) {
            handle_ = CreateFileA(path.data(), GENERIC_READ | GENERIC_WRITE, 0, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
        }
        else {
            handle_ = CreateFileA(path.data(), GENERIC_READ | GENERIC_WRITE, 0, NULL, OPEN_ALWAYS, FILE_FLAG_WRITE_THROUGH, NULL);
        }
        if (handle_ == INVALID_HANDLE_VALUE) {
            return false;
        }
        return true;
    }

    void Seek(int64_t offset, PointerMode fromwhere = PointerMode::kDbFilePointerSet) {
        if (!SetFilePointerEx(handle_, *reinterpret_cast<LARGE_INTEGER*>(&offset), NULL, static_cast<DWORD>(fromwhere))) {
            throw std::ios_base::failure{ "set file pointer failed!" };
        }
    }

    int64_t Tell() {
        LARGE_INTEGER liCurrentPosition;
        liCurrentPosition.QuadPart = 0;
        if (!SetFilePointerEx(handle_, liCurrentPosition, &liCurrentPosition, static_cast<DWORD>(PointerMode::kDbFilePointerCur))) {
            throw std::ios_base::failure{ "get file pointer failed!" };
        }
        return liCurrentPosition.QuadPart;
    }

    size_t Read(void* buf, size_t size) {
        DWORD ret_len;
        BOOL success = ReadFile(handle_, buf, size, &ret_len, NULL);
        if (!success) return 0;
        return ret_len;
    }

    void Write(const void* buf, size_t size) {
        DWORD len;
        if (!WriteFile(handle_, buf, size, &len, NULL)) {
            throw std::ios_base::failure{ "write file failed!" };
        }
    }

    void Sync() {
        if (!FlushFileBuffers(handle_)) {
            throw std::ios_base::failure{ "sync file failed!" };
        }
    }

private:
    void Close() {
        if (handle_ != INVALID_HANDLE_VALUE) {
            CloseHandle(handle_);
            handle_ = INVALID_HANDLE_VALUE;
        }
    }

private:
    HANDLE handle_{ INVALID_HANDLE_VALUE };
};

} // namespace yudb