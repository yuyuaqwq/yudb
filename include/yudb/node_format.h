//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include "yudb/tx_format.h"
#include "yudb/page_format.h"
#include "yudb/slot.h"

namespace yudb {

#pragma pack(push, 1)
enum class NodeType : uint16_t {
    kInvalid = 0,
    kBranch,
    kLeaf,
};

struct OverflowRecord {
    PageId pgid;
};

struct NodeHeader {
    TxId last_modified_txid;
    NodeType type : 2;
    uint16_t count : 14;  // count of slots.
    PageSize space_used;  // excluding the space occupied by deleted records.
    PageOffset data_offset; // the tail of the records.
};

struct NodeStruct {
    NodeHeader header;
    union {
        PageId tail_child;
        uint32_t padding;
        static_assert(sizeof(tail_child) == sizeof(padding));
    };
    Slot slots[1];
};
#pragma pack(pop)

} // namespace