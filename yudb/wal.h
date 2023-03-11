#ifndef YUDB_WAL_H_
#define YUDB_WAL_H_

#include <stdbool.h>
#include <stdint.h>

#include <CUtils/algorithm/crc32.h>

#include <yudb/db_file.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

typedef enum _LogType {
	kLogBegin,
	kLogCommit,
	kLogInsert,
	kLogDelete,
} LogType;

typedef struct _LogData {
	int16_t size;
	uint8_t data[];
} LogData;

typedef struct _LogHeader {
	uint32_t crc32;
	uint32_t size;
	LogType type;
} LogHeader;

typedef struct _LogEntry {
	LogHeader head;
	union {
		LogData key;
		LogData value;
	};
} LogEntry;

void LogAppendBegin(DbFile* log_file);
void LogAppendCommit(DbFile* log_file);
void LogAppendInsert(DbFile* log_file, void* key, int16_t key_size, void* value, int16_t value_size);
void LogAppendDelete(DbFile* log_file, void* key, int16_t key_size);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_WAL_H_
