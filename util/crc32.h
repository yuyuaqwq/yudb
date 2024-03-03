#pragma once

#include <cstdint>

namespace yudb {

/*
https://blog.csdn.net/gongmin856/article/details/77101397
*/


class Crc32 {
public:
    Crc32();
    ~Crc32();

    void Append(const void* buf, size_t size);
    uint32_t End();

private:
    uint32_t value_ = 0xffffffff;
};

} // namespace yudb