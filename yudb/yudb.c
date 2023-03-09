#include "yudb/yudb.h"

#include "yudb/db_file.h"
#include "yudb/pager.h"
#include "yudb/transaction.h"
#include "yudb/bucket.h"

#define PAGE_SIZE 4096
#define CACHE_COUNT 8


bool MetaInfoRead(YuDb* db) {
	if (!DbFileRead(db->db_file, &db->meta_info, sizeof(db->meta_info))) {
		// 初始化数据库必要的页面，meta、free_l0_list、free_l1_list
		uint8_t* empty_page = malloc(PAGE_SIZE);
		memset(empty_page, 0, PAGE_SIZE);

		// meta
		DbFileWrite(db->db_file, empty_page, PAGE_SIZE);
		DbFileSeek(db->db_file, 0, kDbFilePointerEnd);
		DbFileWrite(db->db_file, empty_page, PAGE_SIZE);

		#define Free1Entry uint32_t
		// free0_table
		Free0Entry* free0_table = empty_page;
		free0_table[0].free1_table_max_free = PAGE_SIZE / sizeof(Free1Entry) - 6;
		for (int i = 1; i < PAGE_SIZE / sizeof(Free0Entry); i++) {
			free0_table[i].free1_table_max_free = PAGE_SIZE / sizeof(Free1Entry);
			free0_table[i].free1_table_select = 0;
		}
		DbFileSeek(db->db_file, 0, kDbFilePointerEnd);
		DbFileWrite(db->db_file, empty_page, PAGE_SIZE);
		DbFileSeek(db->db_file, 0, kDbFilePointerEnd);
		DbFileWrite(db->db_file, empty_page, PAGE_SIZE);


		memset(empty_page, 0, PAGE_SIZE);
		// free1_table
		
		Free1Entry* free1_table = empty_page;
		for (int i = 0; i < 6; i++) {
			free1_table[i] = -1;
		}
		#undef Free1Entry

		DbFileSeek(db->db_file, 0, kDbFilePointerEnd);
		DbFileWrite(db->db_file, empty_page, PAGE_SIZE);
		DbFileSeek(db->db_file, 0, kDbFilePointerEnd);
		DbFileWrite(db->db_file, empty_page, PAGE_SIZE);
		free(empty_page);

		// meta
		db->meta_info.magic = 'yudb';
		db->meta_info.page_size = PAGE_SIZE;
		db->meta_info.page_count = 6;
		db->meta_info.txid = 0;
		
		DbFileSeek(db->db_file, 0, kDbFilePointerSet);
		DbFileWrite(db->db_file, &db->meta_info, sizeof(db->meta_info));

		db->meta_info.txid = 1;
		DbFileSeek(db->db_file, PAGE_SIZE, kDbFilePointerSet);
		DbFileWrite(db->db_file, &db->meta_info, sizeof(db->meta_info));
	}
	else {
		MetaInfo temp[2];
		DbFileSeek(db->db_file, 0, kDbFilePointerSet);
		DbFileRead(db->db_file, &temp[0], sizeof(temp[0]));
		DbFileSeek(db->db_file, PAGE_SIZE, kDbFilePointerSet);
		DbFileRead(db->db_file, &temp[1], sizeof(temp[1]));

		if (temp[0].magic != 'yudb' && temp[1].magic != 'yudb') {
			return false;
		}

		// 选择更大事务id的元信息页面
		int i = 0;
		if (temp[0].txid < temp[1].txid || temp[1].txid == 0) {
			i = 1;
		}

		// 检查元信息是否完整，不完整则使用另一个


		memcpy(&db->meta_info, &temp[i], sizeof(db->meta_info));
	}
	return true;
}



YuDb* YuDbOpen(const char* path) {
	YuDb* db = malloc(sizeof(YuDb));
	memset(db, 0, sizeof(*db));
	db->db_file = DbFileOpen(path);
	bool success = false;
	do {
		if (!MetaInfoRead(db)) { break; }
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

