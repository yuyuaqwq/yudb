//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <cassert>
#include <cstring>

#include <span>

namespace yudb {

struct Comparator {
public:
    using FuncPtr = std::strong_ordering(*)(std::span<const uint8_t> key1, std::span<const uint8_t> key2);
    FuncPtr ptr_;
};

inline std::strong_ordering UInt32CompFunc(std::span<const uint8_t> key1, std::span<const uint8_t> key2) {
    assert(key1.size() == sizeof(uint32_t) && key2.size() == sizeof(uint32_t));
    uint32_t key1_ = *reinterpret_cast<const uint32_t*>(key1.data());
    uint32_t key2_ = *reinterpret_cast<const uint32_t*>(key2.data());
    return key1_ <=> key2_;
}

inline std::strong_ordering UInt64CompFunc(std::span<const uint8_t> key1, std::span<const uint8_t> key2) {
    assert(key1.size() == sizeof(uint64_t) && key2.size() == sizeof(uint64_t));
    uint64_t key1_ = *reinterpret_cast<const uint64_t*>(key1.data());
    uint64_t key2_ = *reinterpret_cast<const uint64_t*>(key2.data());
    return key1_ <=> key2_;
}

inline std::strong_ordering ByteArrayCompFunc(std::span<const uint8_t> key1, std::span<const uint8_t> key2) {
    int64_t res = std::memcmp(key1.data(), key2.data(), std::min(key1.size(), key2.size()));
    if (res == 0) {
        if (key1.size() == key2.size()) {
            return std::strong_ordering::equal;
        }
        res = key1.size() - key2.size();
    }
    if (res < 0) {
        return std::strong_ordering::less;
    }
    else {
        return std::strong_ordering::greater;
    }
}

constexpr Comparator UInt32Comparator{ UInt32CompFunc };
constexpr Comparator UInt64Comparator{ UInt32CompFunc };
constexpr Comparator ByteArrayComparator{ ByteArrayCompFunc };

} // namespace yudb
