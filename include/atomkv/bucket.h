//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <atomkv/noncopyable.h>
#include <atomkv/bucket_iterator.h>

namespace atomkv {

class BucketImpl;

class ViewBucket : noncopyable {
public:
    using Iterator = BucketIterator;

public:
    explicit ViewBucket(BucketImpl* bucket);
    ~ViewBucket();

    ViewBucket SubViewBucket(std::string_view key);
    Iterator Get(const void* key_buf, size_t key_size) const;
    Iterator Get(std::string_view key) const;
    Iterator LowerBound(const void* key_buf, size_t key_size) const;
    Iterator LowerBound(std::string_view key) const;

    Iterator begin() const noexcept;
    Iterator end() const noexcept;

    // void Print(bool str = false) const;

protected:
    BucketImpl* const bucket_;
};

class UpdateBucket : public ViewBucket {
public:
    using ViewBucket::ViewBucket;

    UpdateBucket() = delete;
    ~UpdateBucket();

    UpdateBucket SubUpdateBucket(std::string_view key);
    bool DeleteSubBucket(std::string_view key);

    void Put(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size);
    void Put(std::string_view key, std::string_view value);
    bool Delete(const void* key_buf, size_t key_size);
    bool Delete(std::string_view key);
};

} // namespace atomkv