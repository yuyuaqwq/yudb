#include <yudb/meta_info.h>

#include <libyuc/algorithm/crc32.h>

#include <yudb/free_manager/free_manager.h>
#include <yudb/yudb.h>


bool MetaInfoRead(YuDb* db, Config* config) {
    if (!DbFileRead(db->db_file, &db->meta_info, sizeof(db->meta_info))) {
        // 初始化数据库元信息页面，meta
        db->meta_info.magic = 'yudb';
        db->meta_info.min_version = YUDB_VERSION;
        db->meta_info.page_size = config->page_size;
        db->meta_info.page_count = 8;
        db->meta_info.txid = 0;

        PageCount crc32 = Crc32Start();
        crc32 = Crc32Continue(crc32, &db->meta_info, sizeof(db->meta_info) - sizeof(PageCount));
        db->meta_info.crc32 = Crc32End(crc32);

        DbFileSeek(db->db_file, 0, kDbFilePointerSet);
        DbFileWrite(db->db_file, &db->meta_info, sizeof(db->meta_info));

        db->meta_info.txid = 1;
        crc32 = Crc32Start();
        crc32 = Crc32Continue(crc32, &db->meta_info, sizeof(db->meta_info) - sizeof(PageCount));
        db->meta_info.crc32 = Crc32End(crc32);

        DbFileSeek(db->db_file, config->page_size, kDbFilePointerSet);
        DbFileWrite(db->db_file, &db->meta_info, sizeof(db->meta_info));

        db->meta_index = 1;
    }
    else {
        MetaInfo meta_list[2];
        DbFileSeek(db->db_file, 0, kDbFilePointerSet);
        DbFileRead(db->db_file, &meta_list[0], sizeof(meta_list[0]));
        DbFileSeek(db->db_file, config->page_size, kDbFilePointerSet);
        DbFileRead(db->db_file, &meta_list[1], sizeof(meta_list[1]));

        if (meta_list[0].magic != 'yudb' && meta_list[1].magic != 'yudb') {
            return false;
        }

        if (YUDB_VERSION < meta_list[0].min_version) {
            return false;
        }

        // 选择最新的持久化版本元信息页面
        db->meta_index = 0;
        if (meta_list[0].txid < meta_list[1].txid) {
            db->meta_index = 1;
        }

        // 校验元信息是否完整，不完整则使用另一个
        PageCount crc32 = Crc32Start();
        crc32 = Crc32Continue(crc32, &meta_list[db->meta_index], sizeof(meta_list[db->meta_index]) - sizeof(PageCount));
        crc32 = Crc32End(crc32);
        if (crc32 != meta_list[db->meta_index].crc32) {
            if (db->meta_index == 1) { 
                return false; 
            }
            crc32 = Crc32Start();
            crc32 = Crc32Continue(crc32, &meta_list[1], sizeof(meta_list[1]) - sizeof(PageCount));
            crc32 = Crc32End(crc32);
            if (crc32 != meta_list[1].crc32) {
                return false;
            }
        }

        // 页面尺寸不匹配则不允许打开
        if (meta_list[db->meta_index].page_size != config->page_size) {
            return false;
        }
        memcpy(&db->meta_info, &meta_list[db->meta_index], sizeof(db->meta_info));
    }
    return true;
}

bool MetaInfoWrite(YuDb* db, int32_t meta_index) {
    int64_t offset = (int64_t)db->pager.page_size * meta_index;
    DbFileSeek(db->db_file, offset, kDbFilePointerSet);
    PageCount crc32 = Crc32Start();
    crc32 = Crc32Continue(crc32, &db->meta_info, sizeof(db->meta_info) - sizeof(PageCount));
    db->meta_info.crc32 = Crc32End(crc32);
    DbFileWrite(db->db_file, &db->meta_info, sizeof(db->meta_info));
    if (db->config.sync_mode == kConfigSyncFull) {
        DbFileSync(db->db_file);
    }
    return true;
}