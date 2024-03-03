#include "yudb/meta.h"

#include "yudb/version.h"
#include "yudb/db_impl.h"
#include "yudb/crc32.h"

namespace yudb {

Meta::Meta(DBImpl* db) : db_{ db } {};

bool Meta::Load() {
    db_->file().Seek(0, File::PointerMode::kDbFilePointerSet);
    const auto success = db_->file().Read(&meta_struct_, sizeof(meta_struct_));
    if (!success) {
        // Initialize Meta Information
        meta_struct_.sign = YUDB_SIGN;
        meta_struct_.min_version = YUDB_VERSION;
        meta_struct_.page_size = db_->options()->page_size;
        meta_struct_.page_count = 2;
        meta_struct_.txid = 1;
        meta_struct_.root = kPageInvalidId;
        Crc32 crc32;
        crc32.Append(&meta_struct_, kMetaSize - sizeof(uint32_t));
        auto crc32_value = crc32.End();
        meta_struct_.crc32 = crc32_value;

        db_->file().Seek(0, File::PointerMode::kDbFilePointerSet);
        db_->file().Write(&meta_struct_, kMetaSize);

        meta_struct_.txid = 0;
        db_->file().Seek(db_->options()->page_size, File::PointerMode::kDbFilePointerSet);
        db_->file().Write(&meta_struct_, kMetaSize);

        meta_struct_.txid = 1;
        return true;
    }

    // 校验可用元信息
    MetaStruct meta_list[2];
    std::memcpy(&meta_list[0], &meta_struct_, kMetaSize);

    db_->file().Seek(db_->options()->page_size, File::PointerMode::kDbFilePointerSet);
    if (db_->file().Read(&meta_list[1], kMetaSize) != kMetaSize) {
        return false;
    }
    if (meta_list[0].sign != YUDB_SIGN && meta_list[1].sign != YUDB_SIGN) {
        return false;
    }
    if (YUDB_VERSION < meta_list[0].min_version) {
        return false;
    }

    // 选择最新的持久化版本元信息
    cur_meta_index_ = 0;
    if (meta_list[0].txid < meta_list[1].txid) {
        cur_meta_index_ = 1;
    }

    // 校验元信息是否完整，不完整则使用另一个
    Crc32 crc32;
    crc32.Append(&meta_list[cur_meta_index_], kMetaSize - sizeof(uint32_t));
    auto crc32_value = crc32.End();
    if (crc32_value != meta_list[cur_meta_index_].crc32) {
        if (cur_meta_index_ == 1) {
            return false;
        }
        
        crc32.Append(&meta_list[1], kMetaSize - sizeof(uint32_t));
        crc32_value = crc32.End();
        if (crc32_value != meta_list[1].crc32) {
            return false;
        }
    }

    // 页面尺寸不匹配则不允许打开
    if (meta_list[cur_meta_index_].page_size != db_->options()->page_size) {
        return false;
    }
    std::memcpy(&meta_struct_, &meta_list[cur_meta_index_], kMetaSize);
    return true;
}

void Meta::Save() {
    Crc32 crc32;
    crc32.Append(&meta_struct_, kMetaSize - sizeof(uint32_t));
    meta_struct_.crc32 = crc32.End();
    db_->file().Seek(cur_meta_index_ * db_->options()->page_size, File::PointerMode::kDbFilePointerSet);
    db_->file().Write(&meta_struct_, kMetaSize);
}

void Meta::Switch() { 
    cur_meta_index_ = cur_meta_index_ == 0 ? 1 : 0;
}

} // namespace yudb