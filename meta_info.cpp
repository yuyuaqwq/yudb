#include "meta_info.h"

#include "version.h"
#include "db.h"
#include "crc32.h"

namespace yudb {

bool MetaInfor::Load() {
    auto success = db_->file_.Read(&meta_info_, sizeof(meta_info_));
    if (!success) {
        // Initialize Meta Information
        meta_info_.sign = YUDB_SIGN;
        meta_info_.min_version = YUDB_VERSION;
        meta_info_.page_size = kPageSize;
        meta_info_.txid = 0;

        db_->file_.Seek(0, File::PointerMode::kDbFilePointerSet);
        db_->file_.Write(&meta_info_, sizeof(meta_info_));

        return true;
    }

    // Check Meta information
    MetaInfo meta_list[2];
    memcpy(&meta_list[0], &meta_info_, sizeof(meta_list[0]));

    db_->file_.Seek(kPageSize, File::PointerMode::kDbFilePointerSet);
    if (!db_->file_.Read(&meta_list[1], sizeof(meta_list[1]))) {
        return false;
    }
    if (meta_list[0].sign != YUDB_SIGN && meta_list[1].sign != YUDB_SIGN) {
        return false;
    }
    if (YUDB_VERSION < meta_list[0].min_version) {
        return false;
    }

    // 选择最新的持久化版本元信息
    meta_index_ = 0;
    if (meta_list[0].txid < meta_list[1].txid) {
        meta_index_ = 1;
    }

    // 校验元信息是否完整，不完整则使用另一个
    Crc32 crc32;
    crc32.Append(&meta_list[meta_index_], sizeof(meta_list[meta_index_]) - sizeof(uint32_t));
    auto crc32_value = crc32.End();
    if (crc32_value != meta_list[meta_index_].crc32) {
        if (meta_index_ == 1) {
            return false;
        }
        
        crc32.Append(&meta_list[1], sizeof(meta_list[1]) - sizeof(uint32_t));
        crc32_value = crc32.End();
        if (crc32_value != meta_list[1].crc32) {
            return false;
        }
    }

    // 页面尺寸不匹配则不允许打开
    if (meta_list[meta_index_].page_size != kPageSize) {
        return false;
    }
    memcpy(&meta_info_, &meta_list[meta_index_], sizeof(meta_info_));
    return true;
}


} // namespace yudb