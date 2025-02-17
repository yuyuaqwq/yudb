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
#include <cstring>
#include <cassert>

#include <utility>

#include <atomkv/page_format.h>

namespace atomkv {

using SlotId = int16_t;
constexpr SlotId kSlotInvalidId = -1;

constexpr size_t kKeyMaxSize = 0x7fff;
constexpr size_t kValueMaxSize = 0xffffffff;

#pragma pack(push, 1)
struct Slot {
    uint16_t record_offset : 15;
    uint16_t is_overflow_pages : 1;
    uint16_t key_size : 15;
    uint16_t is_bucket : 1;
    union {
        PageId left_child;
        uint32_t value_size;
        static_assert(sizeof(left_child) == sizeof(value_size));
    };
};
#pragma pack(pop)

} // namespace atomkv 