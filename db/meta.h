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

#include <yudb/error.h>
#include <yudb/noncopyable.h>
#include <yudb/meta_format.h>

namespace yudb {

class DBImpl;

class MetaError : public Error {
public:
    using Error::Error;
};

class Meta : noncopyable {
public:
    Meta(DBImpl* db, MetaStruct* meta_struct);
    ~Meta();

    void Init();
    void Load();
    void Save();
    void Switch();
    void Reset(const MetaStruct& meta_struct);

    const auto& meta_struct() const { return *meta_struct_; }
    auto& meta_struct() { return *meta_struct_; }

private:
    DBImpl* const db_;
    MetaStruct* meta_struct_;
    uint32_t cur_meta_index_{ 0 };
};

} // namespace yudb