//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include "meta.h"

#include <wal/crc32.h>

#include <atomkv/version.h>

#include "db_impl.h"

namespace atomkv {

Meta::Meta(DBImpl* db, MetaStruct* meta_struct)
    : db_(db)
    , meta_struct_(meta_struct) {}

Meta::~Meta() = default;

void Meta::Init() {
    cur_meta_index_ = 0;

    auto const first = meta_struct_;
    first->sign = ATOMKV_SIGN;
    first->page_size = db_->options()->page_size;
    first->min_version = ATOMKV_VERSION;
    first->page_count = 2;
    first->txid = 2;
    first->user_root = kPageInvalidId;
    first->free_list_pgid = kPageInvalidId;
    first->free_pair_count = 0;
    first->free_list_page_count = 0;
    Save();

    Switch();
    first->txid = 1;
    Save();

    Switch();
}

void Meta::Load() {
    auto ptr = db_->db_file_mmap().data();

    // Verify available metadata
    auto first = reinterpret_cast<MetaStruct*>(ptr);
    auto second = reinterpret_cast<MetaStruct*>(ptr + db_->options()->page_size);

    if (first->sign != ATOMKV_SIGN && second->sign != ATOMKV_SIGN) {
        throw std::runtime_error("Not a atomkv file.");
    }
    if (ATOMKV_VERSION < first->min_version) {
        throw std::runtime_error("the target database version is too high.");
    }

    const MetaStruct* select;
    // Prioritize selecting the new version
    cur_meta_index_ = 0;
    if (first->txid < second->txid) {
        cur_meta_index_ = 1;
        select = second;
    } else {
        select = first;
    }

    //Verify if the metadata is complete. If it is incomplete, use another one
    wal::Crc32 crc32;
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
            throw std::runtime_error("database is damaged.");
        }
    }

    // Page size requirements are consistent
    if (select->page_size != db_->options()->page_size) {
        throw std::runtime_error("database cannot match system page size.");
    }

    std::memcpy(meta_struct_, select, kMetaSize);
}

void Meta::Save() {
    wal::Crc32 crc32;
    crc32.Append(meta_struct_, kMetaSize - sizeof(uint32_t));
    meta_struct_->crc32 = crc32.End();
    db_->db_file().seekg(cur_meta_index_ * meta_struct_->page_size);
    db_->db_file().write(meta_struct_, kMetaSize);
    db_->db_file().sync();
}

void Meta::Switch() { 
    cur_meta_index_ = cur_meta_index_ == 0 ? 1 : 0;
}

void Meta::Reset(const MetaStruct& meta_struct) {
    CopyMetaInfo(meta_struct_, meta_struct);
}

} // namespace atomkv