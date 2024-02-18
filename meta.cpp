#include "meta.h"

#include "version.h"
#include "db_impl.h"
#include "crc32.h"

namespace yudb {

Meta::Meta(DBImpl* db) : db_{ db } {};

bool Meta::Load() {
    db_->file().Seek(0, File::PointerMode::kDbFilePointerSet);
    const auto success = db_->file().Read(&meta_format_, sizeof(meta_format_));
    if (!success) {
        // Initialize Meta Information
        meta_format_.sign = YUDB_SIGN;
        meta_format_.min_version = YUDB_VERSION;
        meta_format_.page_size = db_->options()->page_size;
        meta_format_.page_count = 2;
        meta_format_.txid = 1;
        meta_format_.root = kPageInvalidId;
        Crc32 crc32;
        crc32.Append(&meta_format_, kMetaSize - sizeof(uint32_t));
        auto crc32_value = crc32.End();
        meta_format_.crc32 = crc32_value;

        db_->file().Seek(0, File::PointerMode::kDbFilePointerSet);
        db_->file().Write(&meta_format_, kMetaSize);

        meta_format_.txid = 0;
        db_->file().Seek(db_->options()->page_size, File::PointerMode::kDbFilePointerSet);
        db_->file().Write(&meta_format_, kMetaSize);

        meta_format_.txid = 1;
        return true;
    }

    // 校验可用元信息
    MetaFormat meta_list[2];
    std::memcpy(&meta_list[0], &meta_format_, kMetaSize);

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
    meta_index_ = 0;
    if (meta_list[0].txid < meta_list[1].txid) {
        meta_index_ = 1;
    }

    // 校验元信息是否完整，不完整则使用另一个
    Crc32 crc32;
    crc32.Append(&meta_list[meta_index_], kMetaSize - sizeof(uint32_t));
    auto crc32_value = crc32.End();
    if (crc32_value != meta_list[meta_index_].crc32) {
        if (meta_index_ == 1) {
            return false;
        }
        
        crc32.Append(&meta_list[1], kMetaSize - sizeof(uint32_t));
        crc32_value = crc32.End();
        if (crc32_value != meta_list[1].crc32) {
            return false;
        }
    }

    // 页面尺寸不匹配则不允许打开
    if (meta_list[meta_index_].page_size != db_->options()->page_size) {
        return false;
    }
    std::memcpy(&meta_format_, &meta_list[meta_index_], kMetaSize);
    return true;
}

void Meta::Save() {
    Crc32 crc32;
    crc32.Append(&meta_format_, kMetaSize - sizeof(uint32_t));
    meta_format_.crc32 = crc32.End();
    db_->file().Seek((!meta_index_) * db_->options()->page_size, File::PointerMode::kDbFilePointerSet);
    db_->file().Write(&meta_format_, kMetaSize);
}

void Meta::Switch() { 
    meta_index_ = !meta_index_;
}

} // namespace yudb