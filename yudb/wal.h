#ifndef YUDB_WAL_H_
#define YUDB_WAL_H_

#include <stdbool.h>
#include <stdint.h>

#include <CUtils/algorithm/crc32.h>
#include <CUtils/container/rb_tree.h>

#include <yudb/db_file.h>
#include <yudb/page.h>

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


typedef struct _Wal {
	DbFile* log_file;
	DbFile* immutable_log_file;
} Wal;

void WalAppendBegin(DbFile* log_file);
void WalAppendCommit(DbFile* log_file);
void WalAppendPut(DbFile* log_file, void* key, int16_t key_size, void* value, int16_t value_size);
void WalAppendDelete(DbFile* log_file, void* key, int16_t key_size);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_WAL_H_
