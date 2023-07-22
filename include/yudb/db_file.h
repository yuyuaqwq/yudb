#ifndef YUDB_DB_FILE_H_
#define YUDB_DB_FILE_H_

#include <stdbool.h>
#include <stdint.h>

#ifdef _MSC_VER
#include <Windows.h>
#else
#endif // _MSC_VER

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

#ifdef _MSC_VER
typedef struct {
  HANDLE file;
} DbFile;
typedef enum {
  kDbFilePointerSet = FILE_BEGIN,
  kDbFilePointerCur = FILE_CURRENT,
  kDbFilePointerEnd = FILE_END,
} DbFilePointerMode;
#else
#endif // _MSC_VER

DbFile* DbFileOpen(const char* path, bool use_system_buf);
void DbFileClose(DbFile* db_file);
bool DbFileSeek(DbFile* db_file, int64_t offset, DbFilePointerMode fromwhere);
int64_t DbFileTell(DbFile* db_file);
bool DbFileRead(DbFile* db_file, void* buf, size_t size);
bool DbFileWrite(DbFile* db_file, void* buf, size_t size);
bool DbFileSync(DbFile* db);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_DB_FILE_H_