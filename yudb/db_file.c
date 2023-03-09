#include <yudb/db_file.h>

#include <stdlib.h>
#include <stdio.h>

#if defined(_MSC_VER)

/*
* 打开文件
*/
DbFile* DbFileOpen(const char* path) {
	DbFile* db_file = (DbFile*)malloc(sizeof(DbFile));
	if (!db_file) {
		return NULL;
	}
	db_file->file = CreateFileA(path, GENERIC_READ | GENERIC_WRITE, 0, NULL, OPEN_ALWAYS, FILE_FLAG_WRITE_THROUGH, NULL);
	if (db_file->file == INVALID_HANDLE_VALUE) {
		free(db_file);
		return NULL;
	}
	return db_file;
}

/*
* 关闭文件
*/
void DbFileClose(DbFile* db_file) {
	if (!db_file) {
		return;
	}
	CloseHandle(db_file->file);
	free(db_file);
}

/*
* 调整文件指针
* Seek再Write，超出文件大小时会自动扩展文件大小并写入，填充字节未定义
*/
bool DbFileSeek(DbFile* db_file, int64_t offset, DbFilePointerMode fromwhere) {
	return SetFilePointerEx(db_file->file, *(LARGE_INTEGER*)&offset, NULL, fromwhere);
}

/*
* 获取文件指针
*/
int64_t DbFileTell(DbFile* db_file) {
	LARGE_INTEGER liCurrentPosition;
	liCurrentPosition.QuadPart = 0;
	SetFilePointerEx(db_file->file, liCurrentPosition, &liCurrentPosition, kDbFilePointerCur);
	return liCurrentPosition.QuadPart;
}

/*
* 读取文件
*/
bool DbFileRead(DbFile* db_file, void* buf, size_t size) {
	DWORD ret_len;
	BOOL success = ReadFile(db_file->file, buf, size, &ret_len, NULL);
	return success && size == ret_len;
}

/*
* 写入文件
*/
bool DbFileWrite(DbFile* db_file, void* buf, size_t size) {
	DWORD len;
	return WriteFile(db_file->file, buf, size, &len, NULL);
}

/*
* 同步文件写入
*/
bool DbFileSync(DbFile* db) {
	return FlushFileBuffers(db->file);		// 代替FILE_FLAG_NO_BUFFERING 
}

#endif // Windows