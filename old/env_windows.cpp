#include "yudb/file.h"

#include "yudb/error.h"

namespace yudb {

File::File() = default;

File::~File() {
	Close();
}

File::File(File&& right) noexcept {
	operator=(std::move(right));
}

void File::operator=(File&& right) noexcept {
	Close();
	handle_ = right.handle_;
	right.handle_ = INVALID_HANDLE_VALUE;
}

bool File::Open(std::string_view path, bool use_system_buf) {
	assert(handle_ == INVALID_HANDLE_VALUE);
	if (use_system_buf) {
		handle_ = CreateFileA(path.data(), GENERIC_READ | GENERIC_WRITE, 0, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
	} else {
		handle_ = CreateFileA(path.data(), GENERIC_READ | GENERIC_WRITE, 0, NULL, OPEN_ALWAYS, FILE_FLAG_WRITE_THROUGH, NULL);
	}
	if (handle_ == INVALID_HANDLE_VALUE) {
		return false;
	}
	return true;
}

void File::Close() {
	if (handle_ != INVALID_HANDLE_VALUE) {
		CloseHandle(handle_);
		handle_ = INVALID_HANDLE_VALUE;
	}
}

void File::Seek(int64_t offset, PointerMode fromwhere) {
	if (!SetFilePointerEx(handle_, *reinterpret_cast<LARGE_INTEGER*>(&offset), NULL, static_cast<DWORD>(fromwhere))) {
		throw IoError{ "set file pointer failed!" };
	}
}

int64_t File::Tell() {
	LARGE_INTEGER liCurrentPosition;
	liCurrentPosition.QuadPart = 0;
	if (!SetFilePointerEx(handle_, liCurrentPosition, &liCurrentPosition, static_cast<DWORD>(PointerMode::kDbFilePointerCur))) {
		throw IoError{ "get file pointer failed!" };
	}
	return liCurrentPosition.QuadPart;
}

size_t File::Read(void* buf, size_t size) {
	DWORD ret_len;
	const BOOL success = ReadFile(handle_, buf, size, &ret_len, NULL);
	if (!success) return 0;
	return ret_len;
}

void File::Write(const void* buf, size_t size) {
	DWORD len;
	if (!WriteFile(handle_, buf, size, &len, NULL)) {
		throw IoError{ "write file failed!" };
	}
}

void File::Sync() {
	if (!FlushFileBuffers(handle_)) {
		throw IoError{ "sync file failed!" };
	}
}

} // namespace yudb