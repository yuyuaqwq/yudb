#include "yudb/wal.h"

static bool LogAppend(DbFile* log_file, LogType type, bool write_buf_size, size_t buf_count, ...) {
	LogEntry entry;
	uint32_t crc32;
	entry.head.crc32 = 0;
	entry.head.size = 0;
	entry.head.type = type;
	crc32 = Crc32Start();
	crc32 = Crc32Continue(crc32, &entry.head, sizeof(entry.head));

	va_list ap;
	va_start(ap, buf_count);
	for (ptrdiff_t i = 0; i < buf_count; i++) {
		void* buf = va_arg(ap, void*);
		uint16_t size = va_arg(ap, uint16_t);
		if (write_buf_size) {
			crc32 = Crc32Continue(crc32, &size, sizeof(size));
		}
		crc32 = Crc32Continue(crc32, buf, size);
	}
	va_end(ap);

	entry.head.crc32 = crc32;
	if (!DbFileWrite(log_file, &entry.head, sizeof(entry.head))) {
		return false;
	}

	bool success = true;
	va_start(ap, buf_count);
	for (ptrdiff_t i = 0; i < buf_count; i++) {
		void* buf = va_arg(ap, void*);
		uint16_t size = va_arg(ap, uint16_t);
		if (write_buf_size && !DbFileWrite(log_file, &size, sizeof(size))) {
			success = false;
			break;
		}
		if (!DbFileWrite(log_file, buf, size)) {
			success = false;
			break;
		}
	}
	va_end(ap);

	return success;
}

void LogAppendBegin(DbFile* log_file) {
	LogAppend(log_file, kLogBegin, false, 0);
}

void LogAppendCommit(DbFile* log_file) {
	LogAppend(log_file, kLogCommit, false, 0);
}

void LogAppendInsert(DbFile* log_file, void* key, int16_t key_size, void* value, int16_t value_size) {
	LogAppend(log_file, kLogInsert, true, 2, key, key_size, value, value_size);
}

void LogAppendDelete(DbFile* log_file, void* key, int16_t key_size) {
	LogAppend(log_file, kLogDelete, true, 1, key, key_size);
}