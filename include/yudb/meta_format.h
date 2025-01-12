//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <cstdint>
#include <cstddef>

#include <yudb/page_format.h>
#include <yudb/tx_format.h>

namespace yudb {

#pragma pack(push, 1)
struct MetaStruct {
    uint32_t sign;
    PageSize page_size;
    uint32_t min_version;
    PageCount page_count;
    PageId user_root;
    PageId free_list_pgid;
    uint32_t free_pair_count;
    PageCount free_list_page_count;
    TxId txid;
    uint32_t crc32;
};
#pragma pack(pop)

constexpr size_t kMetaSize = sizeof(MetaStruct);

inline void CopyMetaInfo(MetaStruct* dst, const MetaStruct& src) {
    dst->user_root = src.user_root;
    dst->free_list_pgid = src.free_list_pgid;
    dst->free_pair_count = src.free_pair_count;
    dst->free_list_page_count = src.free_list_page_count;
    dst->page_count = src.page_count;
    dst->txid = src.txid;
}

} // namespace yudb
