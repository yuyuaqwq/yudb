#include "metaer.h"

#include "version.h"
#include "db.h"
#include "crc32.h"

namespace yudb {

bool Metaer::Load() {
    db_->file_.Seek(0, File::PointerMode::kDbFilePointerSet);
    auto success = db_->file_.Read(&meta_, sizeof(meta_));
    if (!success) {
        // Initialize Meta Information
        meta_.sign = YUDB_SIGN;
        meta_.min_version = YUDB_VERSION;
        meta_.page_size = kPageSize;
        meta_.page_count = 2;
        meta_.tx_id = 1;
        meta_.root = kPageInvalidId;
        Crc32 crc32;
        crc32.Append(&meta_, sizeof(meta_) - sizeof(uint32_t));
        auto crc32_value = crc32.End();
        meta_.crc32 = crc32_value;

        db_->file_.Seek(0, File::PointerMode::kDbFilePointerSet);
        db_->file_.Write(&meta_, sizeof(meta_));

        meta_.tx_id = 0;
        db_->file_.Seek(kPageSize, File::PointerMode::kDbFilePointerSet);
        db_->file_.Write(&meta_, sizeof(meta_));

        return true;
    }

    // Check Meta information
    Meta meta_list[2];
    memcpy(&meta_list[0], &meta_, sizeof(meta_list[0]));

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
    if (meta_list[0].tx_id < meta_list[1].tx_id) {
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
    memcpy(&meta_, &meta_list[meta_index_], sizeof(meta_));
    return true;
}


} // namespace yudb