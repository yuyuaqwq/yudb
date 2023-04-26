#include "yudb/yudb.h"

#include "yudb/db_file.h"
#include "yudb/pager.h"
#include "yudb/transaction.h"
#include "yudb/bucket.h"

//#define PAGE_SIZE 4096
//#define CACHE_COUNT 1024


YuDb* YuDbOpen(const char* path, Config* config) {
	YuDb* db = malloc(sizeof(YuDb));
	memset(db, 0, sizeof(*db));
	db->config;
	if (config->sync_mode == kConfigSyncFull) {
		db->db_file = DbFileOpen(path, false);
	}
	else {
		db->db_file = DbFileOpen(path, true);
	}

	bool success = false;
	do {
		if (!MetaInfoRead(db, config->page_size)) { break; }
		PagerInit(&db->pager, db->meta_info.page_size, db->meta_info.page_count, config->cacher_page_count);
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
	if (db->config->update_mode == kConfigUpdateWal) {
		PagerCleanFreePool(&db->pager);
		PagerWriteAllDirty(&db->pager);
		db->meta_index = (db->meta_index + 1) % 2;
		FreeTableWrite(&db->pager.free_table, db->meta_index);
		MetaInfoWrite(db, db->meta_index);
	}
	DbFileClose(db->db_file);
}