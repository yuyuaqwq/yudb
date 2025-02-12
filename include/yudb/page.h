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
#include <cassert>

#include <utility>

#include <yudb/noncopyable.h>
#include <yudb/page_format.h>

namespace yudb {

class Pager;

class Page : noncopyable {
public:
    Page(Pager* pager, uint8_t* page_buf);
    ~Page();

    Page(Page&& right) noexcept;
    void operator=(Page&& right) noexcept;

    Page AddReference() const;

    auto page_buf() const { return page_buf_; }
    PageId page_id() const { return page_id_; };

protected:
    void Dereference();

protected:
    Pager* const pager_;
    PageId page_id_;
    uint8_t* page_buf_;
};

} // namespace yudb