#include "yudb/meta.h"

#include "yudb/version.h"
#include "yudb/db_impl.h"
#include "yudb/crc32.h"

namespace yudb {

Meta::Meta(DBImpl* db) : db_{ db } {};

Meta::~Meta() = default;

void Meta::Init() {
    cur_meta_index_ = 0;

    auto first = &meta_struct_;
    first->sign = YUDB_SIGN;
    first->page_size = mio::page_size();
    first->min_version = YUDB_VERSION;
    first->page_count = 2;
    first->txid = 1;
    first->user_root = kPageInvalidId;
    first->free_list_pgid = kPageInvalidId;
    first->free_pair_count = 0;
    first->free_list_page_count = 0;

    Crc32 crc32;
    crc32.Append(first, kMetaSize - sizeof(uint32_t));
    first->crc32 = crc32.End();

    Save();
}

bool Meta::Load() {
    auto ptr = db_->db_file_mmap().data();

    // 校验可用元信息
    auto first = reinterpret_cast<MetaStruct*>(ptr);
    auto second = reinterpret_cast<MetaStruct*>(ptr + db_->pager().page_size());

    if (first->sign != YUDB_SIGN && second->sign != YUDB_SIGN) {
        return false;
    }
    if (YUDB_VERSION < first->min_version) {
        return false;
    }

    const MetaStruct* select;
    // 优先选择新版本
    cur_meta_index_ = 0;
    if (first->txid < second->txid) {
        cur_meta_index_ = 1;
        select = second;
    } else {
        select = first;
    }

    // 校验元信息是否完整，不完整则使用另一个
    Crc32 crc32;
    crc32.Append(select, kMetaSize - sizeof(uint32_t));
    auto crc32_value = crc32.End();
    if (crc32_value != select->crc32) {
        if (cur_meta_index_ == 1) {
            cur_meta_index_ = 0;
            select = first;
        } else {
            cur_meta_index_ = 1;
            select = second;
        }
        crc32.Append(select, kMetaSize - sizeof(uint32_t));
        crc32_value = crc32.End();
        if (crc32_value != select->crc32) {
            return false;
        }
    }

    // 页面尺寸要求一致
    if (select->page_size != db_->options()->page_size) {
        return false;
    }

    std::memcpy(&meta_struct_, select, kMetaSize);
    return true;
}

void Meta::Save() {
    Crc32 crc32;
    crc32.Append(&meta_struct_, kMetaSize - sizeof(uint32_t));
    meta_struct_.crc32 = crc32.End();
    db_->db_file().seekg(cur_meta_index_ * meta_struct_.page_size);
    db_->db_file().write(&meta_struct_, kMetaSize);
    db_->db_file().sync();
}

void Meta::Switch() { 
    cur_meta_index_ = cur_meta_index_ == 0 ? 1 : 0;
}

void Meta::Reset(const MetaStruct& meta_struct) {
    CopyMetaInfo(&meta_struct_, meta_struct);
}

} // namespace yudb