#pragma once

#include <filesystem>
#include <fstream>

#ifdef _WIN32
# include <windows.h>
#else
# include <unistd.h>
#endif

#ifndef _WIN32
# define INVALID_HANDLE_VALUE -1
#endif // #ifndef _WIN32

namespace tinyio {

enum class access_mode {
    read,
    write
};

#ifdef _WIN32
    using file_handle_type = HANDLE;
#else
    using file_handle_type = int;
#endif

constexpr static file_handle_type invalid_handle = INVALID_HANDLE_VALUE;

namespace detail {

#ifdef _WIN32
namespace win {

/** Returns the 4 upper bytes of an 8-byte integer. */
inline DWORD int64_high(int64_t n) noexcept {
    return n >> 32;
}

/** Returns the 4 lower bytes of an 8-byte integer. */
inline DWORD int64_low(int64_t n) noexcept {
    return n & 0xffffffff;
}

file_handle_type open_file_helper(const std::filesystem::path& path, const access_mode mode) {
    return ::CreateFileW(path.wstring().c_str(),
            mode == access_mode::read ? GENERIC_READ : GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            0,
            OPEN_ALWAYS,
            FILE_ATTRIBUTE_NORMAL,
            0);
    // FILE_FLAG_WRITE_THROUGH
}
} // win
#endif // _WIN32

/**
 * Returns the last platform specific system error (errno on POSIX and
 * GetLastError on Win) as a `std::error_code`.
 */
inline std::error_code last_error() noexcept {
    std::error_code error;
#ifdef _WIN32
    error.assign(GetLastError(), std::system_category());
#else
    error.assign(errno, std::system_category());
#endif
    return error;
}

file_handle_type open_file(const std::filesystem::path& path, const access_mode mode,
        std::error_code& error) {
    error.clear();
    
    if(path.empty()) {
        error = std::make_error_code(std::errc::invalid_argument);
        return invalid_handle;
    }
#ifdef _WIN32
    const auto handle = win::open_file_helper(path, mode);
#else // POSIX
    const auto handle = ::open(c_str(path),
            (mode == access_mode::read ? O_RDONLY : O_RDWR) | O_CREAT);
#endif
    if(handle == invalid_handle) {
        error = detail::last_error();
    }
    return handle;
}

inline size_t query_file_size(file_handle_type handle, std::error_code& error) {
    error.clear();
#ifdef _WIN32
    LARGE_INTEGER file_size;
    if(::GetFileSizeEx(handle, &file_size) == 0) {
        error = detail::last_error();
        return 0;
    }
	return static_cast<int64_t>(file_size.QuadPart);
#else // POSIX
    struct stat sbuf;
    if(::fstat(handle, &sbuf) == -1) {
        error = detail::last_error();
        return 0;
    }
    return sbuf.st_size;
#endif
}

} // namespace detail

class file {
public:
    file(const file&) = delete;
    void operator=(const file&) = delete;

    void open(const std::filesystem::path& path, const access_mode mode) {
        handle_ = detail::open_file(path, mode, error_);
    }

    bool is_open() {
        return handle_ == invalid_handle;
    }

    void close() {
        if (handle_ != invalid_handle) {
#ifdef _WIN32
            ::CloseHandle(handle_);
#else // POSIX
            ::close(handle_);
#endif
        }
    }

    bool seekg(std::uint64_t pos) {
#ifdef _WIN32
        if (!::SetFilePointerEx(handle_, *reinterpret_cast<LARGE_INTEGER*>(&pos), NULL, FILE_BEGIN)) {
            error_ = detail::last_error();
            return false;
        }
#else // POSIX
        auto cur_pos = lseek(handle_, pos, SEEK_SET);
        if (cur_pos == -1) {
            error_ = detail::last_error();
            return false;
        }
#endif
        return true;
    }

    bool seekg(std::uint64_t off, std::ios_base::seekdir dir) {
#ifdef _WIN32
        DWORD mode;
        switch (dir) {
        case std::ios_base::beg:
            mode = FILE_BEGIN;
            break;
        case std::ios_base::cur:
            mode = FILE_CURRENT;
            break;
        case std::ios_base::end:
            mode = FILE_END;
            break;
        default:
            return false;
        }
        if (!::SetFilePointerEx(handle_, *reinterpret_cast<LARGE_INTEGER*>(&off), NULL, mode)) {
            error_ = detail::last_error();
            return false;
        }
#else // POSIX
        int mode;
        switch (dir) {
        case std::ios_base::beg:
            mode = FILE_BEGIN;
            break;
        case std::ios_base::cur:
            mode = FILE_CURRENT;
            break;
        case std::ios_base::end:
            mode = FILE_END;
            break;
        default:
            return false;
        }
        auto cur_pos = lseek(handle_, pos, SEEK_SET);
        if (cur_pos == -1) {
            error_ = detail::last_error();
            return false;
        }
#endif
    }

    bool tellg() {

    }

private:
    file_handle_type handle_;
    std::error_code error_;
};

}