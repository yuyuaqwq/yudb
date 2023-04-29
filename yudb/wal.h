#ifndef YUDB_WAL_H_
#define YUDB_WAL_H_

#include <stdbool.h>
#include <stdint.h>

#include <CUtils/algorithm/crc32.h>
#include <CUtils/container/rb_tree.h>
#include <CUtils/container/vector.h>

#include <yudb/db_file.h>
#include <yudb/page.h>
#include <yudb/write_queue.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

typedef enum _LogType {
	kLogBegin,
	kLogCommit,
	kLogPut,
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


CUTILS_CONTAINER_VECTOR_DECLARATION(WalBuf, uint8_t)

typedef struct _WalManager {
	DbFile* log_file;
	DbFile* immutable_log_file;
	WalBufVector log_buf;
	char* db_wal_path;

	WriteQueue write_queue;
} WalManager;

void WalInit(WalManager* wal, const char* db_path);
void WalAppendBeginLog(WalManager* log_file);
void WalAppendCommitLog(WalManager* log_file);
void WalAppendPutLog(WalManager* log_file, void* key, int16_t key_size, void* value, int16_t value_size);
void WalAppendDeleteLog(WalManager* log_file, void* key, int16_t key_size);
void WalCrashRecovery(WalManager* log_file);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_WAL_H_
