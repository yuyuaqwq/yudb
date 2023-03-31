#include "yudb/yudb.h"

#include "yudb/db_file.h"
#include "yudb/pager.h"
#include "yudb/transaction.h"
//#include "yudb/bucket.h"

#define PAGE_SIZE 4096
#define CACHE_COUNT 1024



YuDb* YuDbOpen(const char* path, YuDbSyncMode sync_mode) {
	YuDb* db = malloc(sizeof(YuDb));
	memset(db, 0, sizeof(*db));
	if (sync_mode == kYuDbSyncFull) {
		db->db_file = DbFileOpen(path, false);
	}
	else {
		db->db_file = DbFileOpen(path, true);
	}
	db->sync_mode = sync_mode;

	bool success = false;
	do {
		if (!MetaInfoRead(db, PAGE_SIZE)) { break; }
		PagerInit(&db->pager, db->meta_info.page_size, db->meta_info.page_count, CACHE_COUNT);
		TxManagerInit(&db->tx_manager);
		success = true;
	} while (false);
	if (!success) {
		DbFileClose(db->db_file);
		free(db);
		return NULL;
	}
	return db;
}

void YuDbClose(YuDb* db) {
	if (db->update_mode == kYuDbUpdateWal) {
		PagerWriteAllDirty(&db->pager);
		db->meta_index = (db->meta_index + 1) % 2;
		FreeTableWrite(&db->pager.free_table, db->meta_index);
		MetaInfoWrite(db, db->meta_index);
	}
	DbFileClose(db->db_file);
}