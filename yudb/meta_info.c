#include "yudb/meta_info.h"

#include "yudb/free_table.h"
#include "yudb/yudb.h"

bool MetaInfoRead(YuDb* db, uint16_t page_size) {
	if (!DbFileRead(db->db_file, &db->meta_info, sizeof(db->meta_info))) {
		// 初始化数据库必要的页面，meta、free_l0_list、free_l1_list
		uint8_t* empty_page = malloc(page_size);
		memset(empty_page, 0, page_size);

		// meta
		DbFileWrite(db->db_file, empty_page, page_size);
		DbFileSeek(db->db_file, 0, kDbFilePointerEnd);
		DbFileWrite(db->db_file, empty_page, page_size);

		// free0_table
		Free0Table* free0_table = empty_page;
		Free0TableInit(free0_table, page_size);
		Free0StaticList* static_list = Free0TableGetStaticList(free0_table);

		DbFileSeek(db->db_file, 0, kDbFilePointerEnd);
		DbFileWrite(db->db_file, empty_page, page_size);
		DbFileSeek(db->db_file, 0, kDbFilePointerEnd);
		DbFileWrite(db->db_file, empty_page, page_size);

		// free1_table
		memset(empty_page, 0, page_size);
		Free1Table* free1_table = empty_page;
		Free1TableInit(free1_table, page_size);
		// 前6个页面不可分配
		Free1TableAlloc(free1_table, 4);
		Free1TableAlloc(free1_table, 2);

		
		DbFileSeek(db->db_file, 0, kDbFilePointerEnd);
		DbFileWrite(db->db_file, empty_page, page_size);
		DbFileSeek(db->db_file, 0, kDbFilePointerEnd);
		DbFileWrite(db->db_file, empty_page, page_size);
		free(empty_page);

		// meta
		db->meta_info.magic = 'yudb';
		db->meta_info.min_version = YUDB_VERSION;
		db->meta_info.page_size = page_size;
		db->meta_info.page_count = 6;
		db->meta_info.txid = 0;

		DbFileSeek(db->db_file, 0, kDbFilePointerSet);
		DbFileWrite(db->db_file, &db->meta_info, sizeof(db->meta_info));

		db->meta_info.txid = 1;
		DbFileSeek(db->db_file, page_size, kDbFilePointerSet);
		DbFileWrite(db->db_file, &db->meta_info, sizeof(db->meta_info));

		db->meta_index = 1;
	}
	else {
		MetaInfo meta_list[2];
		DbFileSeek(db->db_file, 0, kDbFilePointerSet);
		DbFileRead(db->db_file, &meta_list[0], sizeof(meta_list[0]));
		DbFileSeek(db->db_file, page_size, kDbFilePointerSet);
		DbFileRead(db->db_file, &meta_list[1], sizeof(meta_list[1]));

		if (meta_list[0].magic != 'yudb' && meta_list[1].magic != 'yudb') {
			return false;
		}

		if (YUDB_VERSION < meta_list[0].min_version) {
			return false;
		}

		// 选择更大事务id的元信息页面
		db->meta_index = 0;
		if (meta_list[0].txid < meta_list[1].txid || meta_list[1].txid == 0) {
			db->meta_index = 1;
		}

		// 检查元信息是否完整，不完整则使用另一个

		// 页面尺寸不匹配则不允许打开
		if (meta_list[db->meta_index].page_size != page_size) {
			return false;
		}
		memcpy(&db->meta_info, &meta_list[db->meta_index], sizeof(db->meta_info));
	}
	return true;
}

bool MetaInfoWrite(YuDb* db, int32_t meta_index) {
	int64_t offset = (int64_t)db->pager.page_size * meta_index;
	DbFileSeek(db->db_file, offset, kDbFilePointerSet);
	DbFileWrite(db->db_file, &db->meta_info, sizeof(db->meta_info));
	if (db->config->sync_mode == kConfigSyncFull) {
		DbFileSync(db->db_file);
	}
}